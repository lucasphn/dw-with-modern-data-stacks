{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {# seeds go in a global `ouro` schema #}
    {% if node.resource_type == 'seed' %}
        {{ custom_schema_name | trim }}

    {# ouro models go in a global `ouro` schema #}
    {% elif 'ouro' in node.path %}
        {{ 'ouro' }}

    {# silver models  go in a global `silver` schema #}
    {% elif 'silver' in node.path %}
        {{ 'silver' }}

    {# gold models  go in a global `gold` schema #}
    {% elif 'gold' in node.path %}
        {{ 'gold' }}

    {# non-specified schemas go to the default target schema #}
    {% elif custom_schema_name is none %}
        {{ default_schema }}

    {# specified custom schema names go to the schema name prepended with the default schema name in prod #}
    {% elif target.name == 'prod' %}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {# specified custom schemas go to the default target schema for non-prod targets #}
    {% else %}
        {{ default_schema }}
    {% endif %}

{% endmacro %}
