{% macro recursive_create_table_as(relation, sql, depth) -%}
  {% do log('START insert recursion ' + depth|string + ' data', True) %}
  {% if depth == 0 %}
    {% set re = modules.re %}
    {% set (anchor, recursive) = re.split('__RECURSIVE__', sql, maxsplit=1) %}
    {% set sql = anchor %}
    {% set sql = recursive_add_depth(sql, 0)-%}
    {% call statement('main') -%}
      {{ create_table_as(False, relation, sql) }}
    {%- endcall %}
    {% set rows_in_depth = recursive_rows_in_depth(relation, depth) -%}
    {% do log('OK inserted recursion ' + depth|string + ' data by adding ' + rows_in_depth|string + ' new rows.', True) %}
    {% do recursive_create_table_as(relation, recursive, 1) %}
  {% elif depth > 0 %}
    {% set sql_depth = recursive_add_depth(sql, depth) -%}
    {% set sql_depth = sql_depth|replace('__THIS__', '(SELECT * FROM ' + relation|string + ' WHERE "__depth" = ' + (depth-1)|string + ')') %}
    {% call statement('recursion_'+depth|string) -%}
      {{ recursive_insert(relation, sql_depth) }}
    {%- endcall %}
    {% set rows_in_depth = recursive_rows_in_depth(relation, depth) -%}
    {% do log('OK inserted recursion ' + depth|string + ' data by adding ' + rows_in_depth|string + ' new rows.', True) %}
    {% if rows_in_depth > 0 %}
      {% do recursive_create_table_as(relation, sql, depth+1) %}
    {% else %}
      {# -- remove __depth column #}
      {% do trino__alter_relation_add_remove_columns(relation, none, [{'name':'__depth'}]) %}
    {% endif %}
  {% endif %}
{% endmacro %}

-- wrapper, which adds "__depth" into columns
{% macro recursive_add_depth(sql, depth) -%}
  select  
    sql.*,
    {{ depth }} as "__depth"
  from
    ({{ sql }}) as sql
{% endmacro %}

-- insert into
{% macro recursive_insert(relation, sql) -%}
  insert into {{ relation }}
  {{ sql }}
{% endmacro %}

-- check rows
{% macro recursive_rows_in_depth(relation, depth) -%}
  {% set recursion_check_rows_query %}
    select count(*) as rows_in_depth from {{ relation }} where __depth = {{ depth }}
  {% endset -%}
  {% set results = run_query(recursion_check_rows_query) %}
  {{ return(results.columns[0].values()[0]) }}
{% endmacro %}

-- mainly standard dbt-trino.table materialization
{% materialization recursive, adapter='trino' %}  
  -- get on_table_exists flag and make sure its valid
  {%- set on_table_exists = config.get('on_table_exists', 'rename') -%}
  {% if on_table_exists not in ['rename', 'drop'] %}
      {%- set log_message = 'Invalid value for on_table_exists (%s) specified. Setting default value (%s).' % (on_table_exists, 'rename') -%}
      {% do log(log_message) %}
      {%- set on_table_exists = 'rename' -%}
  {% endif %}

  -- setup names for identifiers
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  -- current table, which needs to be renamed or dropped
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  -- new table, which is generated
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {% if on_table_exists == 'rename' %}
      -- temporary table, while building the new target.
      {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                          schema=schema,
                                                          database=database,
                                                          type='table') -%}

      -- old table
      {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
      {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                    schema=schema,
                                                    database=database,
                                                    type=backup_relation_type) -%}
     
      -- not needed part of the original macro (maybe for a future release or some old code)
      {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
      {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

        -- drop the temp relations if they exists for some reason
      {{ adapter.drop_relation(intermediate_relation) }}
      {{ adapter.drop_relation(backup_relation) }}
  {% endif %}
  -- relations target_relation, intermediate_relation, backup_relation, old_relation are set up
  -- tables intermediate_relation and backup_relation do not exist

  -- starting by running pre hooks
  {{ run_hooks(pre_hooks) }}

  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- creating the target relation
  {% if on_table_exists == 'rename' %}
      {#-- build model using our new logic #}
      {% do recursive_create_table_as(intermediate_relation, sql, 0) %}

      {#-- cleanup #}
      {% if old_relation is not none %}
          {{ adapter.rename_relation(old_relation, backup_relation) }}
      {% endif %}

      {{ adapter.rename_relation(intermediate_relation, target_relation) }}

      {#-- finally, drop the existing/backup relation after the commit #}
      {{ drop_relation_if_exists(backup_relation) }}

  {% elif on_table_exists == 'drop' %}
      {#-- cleanup #}
      {%- if old_relation is not none -%}
          {{ adapter.drop_relation(old_relation) }}
      {%- endif -%}

      {#-- build model using our new logic #}
      {% do recursive_create_table_as(intermediate_relation, sql, 0) %}
  {% endif %}

  -- finishing up, by adding table comments, grants and running post hooks
  {% do persist_docs(target_relation, model) %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}