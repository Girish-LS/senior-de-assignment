{#
  Use the model's custom schema verbatim instead of appending it to the
  target schema.

  dbt's default `generate_schema_name` concatenates: a profile with
  `schema: transactions` and a model with `+schema: silver` produces
  `transactions_silver`. That is deliberate in dbt - it keeps developers'
  schemas separate in a shared warehouse - but here it obscured the medallion
  layout, giving `raw` for bronze alongside `transactions_staging` and
  `transactions_marts`. Two naming mechanisms in one warehouse is a reviewer's
  question waiting to happen.

  Overriding it yields `bronze`, `silver`, `gold`: the layer names the layers
  actually have.

  The trade-off is real and worth naming. dbt's default exists so two
  developers running the same project in one warehouse do not overwrite each
  other. With this override they would. In a team that matters, and the fix is
  to branch on `target.name` - custom schema verbatim for production,
  prefixed with the developer's name otherwise. At one developer and one
  warehouse, clarity of the layer names wins.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
