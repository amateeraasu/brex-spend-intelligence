
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dept_category_month
from "postgres"."mart_mart"."spend_mart"
where dept_category_month is null



  
  
      
    ) dbt_internal_test