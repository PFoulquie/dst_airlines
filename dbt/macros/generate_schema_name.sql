{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {#- Si on définit un schema dans le dbt_project.yml, on l'utilise direct -#}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {#- Sinon on utilise le schema du profil (silver) -#}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}