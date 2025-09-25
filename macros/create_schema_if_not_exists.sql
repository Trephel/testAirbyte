{% macro create_schema_if_not_exists(schema_name) %}
  {% if execute %}
    {% set create_schema_sql %}
      CREATE SCHEMA IF NOT EXISTS {{ target.database }}.{{ schema_name }};
    {% endset %}
    
    {% do run_query(create_schema_sql) %}
    {{ log("Schema " ~ schema_name ~ " créé ou déjà existant", info=true) }}
  {% endif %}
{% endmacro %}