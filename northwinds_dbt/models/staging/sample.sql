{% for i in range(3) %}
  select {{ i }} as number union all 
{% endfor %}