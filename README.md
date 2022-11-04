Add [package](https://docs.getdbt.com/docs/build/packages#git-packages) to your dbt:

packages.yml
``` yaml
packages:
  - git: "https://github.com/christopherscholz/dbt_trino_recursive_materialization.git"
```

## Example Usage
For some example data:

hierarchy.sql
``` sql
select
  b."id",
  if((b.id +1) % 20 != 0, b.id+1) as "parent_id"
from
  -- result of sequence function must not have more than 10000 entries
  unnest(sequence(0, 9999)) as a(id), 
  unnest(sequence((a.id)*1000, (a.id)*1000+999)) as b(id)
```

you can use the model like this:

hierarchy_resolved.sql
``` sql
--  anchor member
select
  "base"."id",
  "base"."parent_id"
from
  {{ ref('hierarchy') }} as "base"

__RECURSIVE__
-- recursive member that references __THIS__.
select
  "child"."id",
  "parent"."parent_id"
from
  __THIS__ as "child"
  inner join {{ ref('hierarchy') }} as "parent" on
    "child"."parent_id" = "parent"."id"
where
  "parent"."parent_id" is not null
```

`__RECURSIVE__` and `__THIS__` must be in the model.