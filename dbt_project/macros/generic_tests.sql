{#
  Local implementations of the two dbt_utils tests this project uses.

  Why not the package
  -------------------
  `dbt deps` resolves packages from hub.getdbt.com, which is blocked on
  corporate networks - the proxy returns 403. A project that cannot run
  `dbt deps` cannot run at all, so depending on the package would make the
  dbt path unrunnable in exactly the environment it is meant to demonstrate.

  Both tests are a handful of lines. Vendoring them keeps the project
  self-contained and removes an install step, at the cost of maintaining two
  small macros. On a network with hub access, deleting these and restoring
  packages.yml is a two-line change.
#}


{% test expression_is_true(model, expression, column_name=None) %}
{#
  Fails for any row where `expression` is not true.

  When applied under a column, the expression may reference that column by
  name. Rows where the expression evaluates to NULL are treated as failures,
  because an assertion that cannot be evaluated has not been satisfied - the
  usual reason a silent data problem survives a test suite.
#}

select *
from {{ model }}
where not (
    {% if column_name %}
        {{ column_name }} {{ expression }}
    {% else %}
        {{ expression }}
    {% endif %}
)
or (
    {% if column_name %}
        {{ column_name }} {{ expression }}
    {% else %}
        {{ expression }}
    {% endif %}
) is null

{% endtest %}


{% test unique_combination_of_columns(model, combination_of_columns) %}
{#
  Fails when the listed columns are not unique together. Used to assert the
  grain of daily_account_summary: exactly one row per account per date.

  This is the single most important assertion on the mart. A broken grain
  double-counts money, which is the failure that destroys trust in a figure
  and is invisible until someone reconciles by hand.
#}

{%- set column_list = combination_of_columns | join(", ") -%}

select
    {{ column_list }},
    count(*) as n_records
from {{ model }}
group by {{ column_list }}
having count(*) > 1

{% endtest %}
