``` sql
select
  b."id",
  if((b.id +1) % 20 != 0, b.id+1) as "parent_id"
from
  -- result of sequence function must not have more than 10000 entries
  unnest(sequence(0, 9999)) as a(id), 
  unnest(sequence((a.id)*1000, (a.id)*1000+999)) as b(id)
```

``` sql
--  anchor member
select
  "base"."id",
  "base"."parent_id"
from
  {{ ref('hierarchy_20') }} as "base"

__RECURSIVE__
-- recursive member that references __THIS__.
select
  "child"."id",
  "parent"."parent_id"
from
  __THIS__ as "child"
  inner join {{ ref('hierarchy_20') }} as "parent" on
    "child"."parent_id" = "parent"."id"
where
  "parent"."parent_id" is not null
```