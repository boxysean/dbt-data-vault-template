
with source as (

    -- FIXED A TYPO HERE!!!
    select * from {{ source('salesforce', 'opportunites') }}  

),

renamed as (

    select
    
        BATCHID,
        OPPURTUNITYID as OPPORTUNITYID,
        COMPANEXTID as ACCOUNTID,
        to_decimal(replace(replace(AMOUNT,'$',''),',',''),9,2) as AMOUNT,
        PROJECT_NAME,
        OPPURTUNITY_NAME as OPPORTUNITY_NAME,
        STAGE,
        CLOSE_DATE,
        DATECREATED,
        MODIFIEDDATE

    from source

)

select * from renamed