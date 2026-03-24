
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    dept_category_month as unique_field,
    count(*) as n_records

from "postgres"."mart_mart"."spend_mart"
where dept_category_month is not null
group by dept_category_month
having count(*) > 1



  
  
      
    ) dbt_internal_test