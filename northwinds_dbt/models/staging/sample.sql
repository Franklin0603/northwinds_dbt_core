-- {% for i in range(3) %}
--   select {{ i }} as number union all 
-- {% endfor %}

{% for num in range(6)%}
  
  {% if num % 2 == 0 %}
     {% set value = 'fizz' %}
  {% else %}
     {%set value = 'buzz'%}

  SELECT 
    {{ num }} as number,
    '{{ value }}' as output
    {% if not loop.last %} union all {% endif %} 
  {% endif %} 

{% endfor %}