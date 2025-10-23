-- Remove duplicates and nulls from a table
{% macro remove_duplicates_and_nulls(table_name, id_column, not_null_columns) %}
    SELECT *
    FROM {{ table_name }}
    WHERE {{ id_column }} NOT IN (
        SELECT {{ id_column }}
        FROM {{ table_name }}
        GROUP BY {{ id_column }}
        HAVING COUNT(*) > 1
    )
    {% for col in not_null_columns %}
        AND {{ col }} IS NOT NULL
    {% endfor %}
{% endmacro %}
