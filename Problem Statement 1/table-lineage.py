# Define a function to fetch direct dependencies of a given table within the database.
# start_table : datamart table as input
# intermedidate_table : dependency table of start_table , will be same in Level 1
def fetch_dependencies(start_table, intermediate_table, level):
    # SQL query to find tables that the current table depends on
    table_filter = f"'{intermediate_table}'"
    query = f"SELECT upstream_table_name as upstream_table_name FROM final_lineage WHERE table_name = {table_filter}"
    dependencies = spark.sql(query).collect()  # Execute the SQL query using Spark
    results = []
    for row in dependencies:
        # Create a list for each dependency providing the datamart_table, intermediate_table, and the upstream_table it depends on
        results.append([start_table, intermediate_table, row.upstream_table_name, level])
    return results

# Function to process lineage (dependencies' chain) of a datamart table up to a maximum level of dependencies
def process_lineage(start_table, max_level):
    results_main = []  # List to store all found dependencies
    level = 1  # Initialize the starting level (root node)
    # Fetch initial dependencies for the start_table
    results_main.extend(fetch_dependencies(spark, start_table, start_table, level))

    # Loop through each level until the maximum specified level is reached
    for current_level in range(2, max_level + 1):
        current_results = []
        for entry in results_main:
            if entry[3] == current_level - 1:  # Only consider dependencies from the previous level
                fetched_results = fetch_dependencies(spark, entry[0], entry[2], current_level)
                current_results.extend(fetched_results)
        results_main.extend(current_results)
    return results_main

# Function to process lineage for multiple start tables from input list
def process_multiple_tables(table_list, max_levels):
    total_results = []

    # Process each table in the provided list of tables
    for start_table in table_list:
        table_results = process_lineage(spark, start_table, max_levels)
        total_results.extend(table_results)

    # Convert the accumulated results into a Spark DataFrame
    df = spark.createDataFrame(total_results, ["Datamart Table", "Intermediate Target", "Upstream Dependence", "level"])
    # Write the results into a CSV file stored in an S3 bucket
    df.coalesce(1).write.mode('append').option("header", "true").csv("s3://idl-finance-sandbox-uw2-processing-fin-prd/kprasad7/sox_output_test.csv")

# Main execution entry point
if __name__ == "__main__":
    # spark.read.option("header", True)\
    #     .csv("s3://idl-finance-sandbox-uw2-processing-fin-prd/sdas24/lineage/Finance_Lineage_ImmediateLineage.csv")\
    #     .selectExpr("tgt as table_name", "src as upstream_table_name")\
    #     .createOrReplaceTempView("final_lineage")


    sql_query="""select distinct qualifiedName as table_name, get_json_object(input_tables, '$.qualifiedName') as upstream_table_name from
              (select test.qualifiedName, explode(pipelineobservedlineageinputs) as input_tables from (
               select from_json(pipelineobservedlineageoutputs[0], 'qualifiedName string, guid string, type string') as test, * from mdr_export.pipeline_metadata 
               where pipelineobservedlineageoutputs is not null or pipelinedeclaredlineageoutputs is not null) 
               where lower(test.qualifiedName) in ('{}'))
               where qualifiedName <> get_json_object(input_tables, '$.qualifiedName') and (lower(split_part(get_json_object(input_tables, '$.qualifiedName'), '.',1)) not like 'finance_sandbox%' and lower(split_part(get_json_object(input_tables, '$.qualifiedName'), '.',1)) not like 'finance_stg%')"""
    
    list_of_start_tables = ['finance_dm.dim_sales_order_status', 'finance_dm.dim_sales_order_types', 'finance_dm.fact_billing_ar', 'finance_dm.fact_sales_units_dly_hyp', 'finance_dm.fact_sbscrptn_dd_pymnt_fail', 'finance_dm.fact_sbscrptn_dscnt', 'finance_dm.fact_sbscrptn_event', 'finance_dm.fact_sbscrptn_item', 'finance_dm.fact_sbscrptn_item_trnsfr', 'finance_dm.fact_sbscrptn_tax', 'finance_dm.fact_sbscrptn_tax_jursdctn', 'finance_dm.fact_totalsubs_actcnt_dly', 'finance_dwh.dw_bill_prof_extract', 'finance_dwh.dw_brm_bill_item', 'finance_dwh.dw_item_transfer_detail', 'finance_dwh.dw_item_usage', 'finance_dwh.dw_item_usage_details', 'finance_dwh.dw_ledger_report_gl', 'finance_dwh.dw_order_extract', 'finance_dwh.dw_profile_vat_status', 'finance_dwh.dw_revenue_ldgr', 'finance_dwh.dw_revstream_events', 'finance_dwh.dw_revstream_header', 'finance_dwh.dw_revstream_lines', 'finance_dwh.dw_sbscrptn_dscnt', 'finance_dwh.dw_sbscrptn_event', 'finance_dwh.dw_sbscrptn_item', 'finance_dwh.dw_sbscrptn_item_preprocess', 'finance_dwh.dw_sbscrptn_payment_fail', 'finance_dwh.dw_sbscrptn_tax', 'finance_dwh.dw_sbscrptn_tax_jrsdctns', 'finance_rpt.brm_sales_tax_audit', 'finance_rpt.brmsnp_aging_1', 'finance_rpt.brmsnp_aging_2', 'finance_rpt.brmsnp_cc_dtl_1', 'finance_rpt.brmsnp_cc_dtl_2', 'finance_rpt.rpt_asset_extract', 'finance_rpt.rpt_asset_reporting', 'finance_rpt.rpt_asset_reporting_summary', 'finance_rpt.rpt_brm_item_detail', 'finance_rpt.rpt_brm_pp_noasset_extract', 'finance_rpt.rpt_ers_ent_noasset_extract', 'finance_rpt.rpt_finance_payout', 'finance_rpt.rpt_intu_srv_collection_status_v', 'finance_rpt.rpt_item_aging', 'finance_rpt.rpt_ldgr_rpt_bld_unbld', 'finance_rpt.rpt_obill_entitlement', 'finance_rpt.rpt_offer_details', 'finance_rpt.rpt_oii_entitlement', 'finance_rpt.rpt_order_item_extract', 'finance_rpt.rpt_pim_accrual', 'finance_rpt.rpt_pim_invoice', 'finance_rpt.rpt_pms_accountant_partners', 'finance_rpt.rpt_webs_entitlement', 'finance_dwh.ego_mtl_sy_items_ext_b_attr_flat', 'finance_dm.dim_alt_hier_cgd_expense', 'finance_dm.dim_alt_product_hierarchy', 'finance_dm.dim_alt_rvnu_prod_hier', 'finance_dm.dim_alt_sales_channel_master', 'finance_dm.dim_asset_location', 'finance_dm.dim_cgd_enabled', 'finance_dm.dim_exchange_rate', 'finance_dm.dim_fixed_asset', 'finance_dm.dim_po_vendor', 'finance_dm.dim_project_by_task', 'finance_dm.dim_supplier', 'finance_dm.odi_expense_fx_rate', 'finance_dm.dim_affiliate', 'finance_dm.dim_asset', 'finance_dm.dim_campaign', 'finance_dm.dim_channel', 'finance_dm.dim_charge', 'finance_dm.dim_coa_company', 'finance_dm.dim_coa_department', 'finance_dm.dim_coa_group', 'finance_dm.dim_coa_group_hier', 'finance_dm.dim_coa_natural_account', 'finance_dm.dim_country', 'finance_dm.dim_currency', 'finance_dm.dim_gep_supplier_business_desc', 'finance_dm.dim_icp_offer', 'finance_dm.dim_intuit_item', 'finance_dm.dim_ledgers', 'finance_dm.dim_offer', 'finance_dm.dim_offer_promotion', 'finance_dm.dim_order_status', 'finance_dm.dim_order_types', 'finance_dm.dim_package_offer', 'finance_dm.dim_product', 'finance_dm.dim_product_hierarchy', 'finance_dm.dim_product_hierarchy_vw', 'finance_dm.dim_product_optional', 'finance_dm.dim_psp_company', 'finance_dm.dim_psp_employee', 'finance_dm.dim_psp_ledger_account', 'finance_dm.dim_psp_transaction', 'finance_dm.dim_retail_customer', 'finance_dm.dim_retail_item', 'finance_dm.dim_sales_channel', 'finance_dm.dim_sbscrptn_customer', 'finance_dm.dim_sbscrptn_offer', 'finance_dm.dim_sbscrptn_offer_package', 'finance_dm.dim_sbscrptn_package', 'finance_dm.dim_sbscrptn_service', 'finance_dm.dim_scenario', 'finance_dm.dim_time_by_day_vw', 'finance_dm.dim_treatment', 'finance_dm.dw_product_hierarchy', 'finance_dm.tto_orders_paid_not_filed', 'finance_dwh.dw_edition_rank', 'finance_dwh.dw_party_nam_mstr', 'finance_dwh.dw_prepaid_bank_cutover', 'finance_dwh.dw_product_downgrade', 'finance_dwh.dw_product_hierarchy', 'finance_dwh.dw_product_vp_lookup', 'finance_dwh.dw_sales_chnl_mstr', 'finance_dwh.lkp_coa_group_hier', 'finance_rpt.plus_coterm_101_def', 'finance_dm.fact_exp_po_details', 'finance_dm.fact_expense', 'finance_dm.fact_fixed_asset_deprn', 'finance_dwh.dw_fa_approver', 'finance_dwh.dw_hr_operating_units', 'finance_dwh.dw_po_requisition', 'finance_dwh.dw_purchase_order', 'finance_dwh.dw_req_po_apinv_link', 'finance_dwh.dw_order_shifted_attributes', 'finance_dwh.dw_orders', 'finance_dm.abc_merecon_tieout', 'finance_dm.fact_psp_assisted_units', 'finance_dm.fact_psp_ledger_balance', 'finance_dm.fact_psp_transaction', 'finance_dm.fact_psp_transaction_tax', 'finance_dwh.dw_psp_dd4v_hldng', 'finance_dwh.dw_psp_events_hldng', 'finance_rpt.rpt_psp_entitlements', 'finance_dm.fact_order_cycle', 'finance_dm.fact_sales_order_line', 'finance_dwh.dw_csldt_rma_order_lines', 'finance_dwh.dw_oe_order_headers', 'finance_dwh.dw_oe_order_lines', 'finance_rpt.ref_cr_memo', 'finance_rpt.ref_cr_memo_lines', 'finance_rpt.rpt_ocfulfillednotinvoiced', 'finance_dm.fact_product_summary_dly', 'finance_rpt.rpt_booking_detail', 'finance_rpt.rpt_fulfillednotinv_nonshippab', 'finance_rpt.rpt_fulfillednotinvoiced_hold', 'finance_rpt.rpt_fulfillednotinvoiced_shippable', 'finance_rpt.rpt_hold_detail', 'finance_rpt.rpt_noetixinv4_itm_oe_attrbs', 'finance_rpt.rpt_om_exception', 'finance_rpt.rpt_om_exception_fullrefresh', 'finance_rpt.rpt_qrytibcoimportfirstorderrownum', 'finance_rpt.rpt_qrytibcoimportpivotdata', 'finance_rpt.rpt_qryuniqueordercount', 'finance_rpt.rpt_quniorderlinesreportdetail', 'finance_rpt.rpt_tbl_itemcrossreference', 'finance_rpt.rpt_tbl_itemgroupcategory', 'finance_rpt.rpt_tblbu_orderlineunique', 'finance_rpt.rpt_tblbu_orderlineuniquefuture', 'finance_rpt.rpt_tbldate_in_queue', 'finance_rpt.rpt_tblorderlines', 'finance_rpt.rpt_tblorderlinesreportdetail', 'finance_rpt.rpt_tblorderlinesreportdetailctg', 'finance_rpt.rpt_tblorderlinesreportdetailpas', 'finance_rpt.rpt_tblorderlineunique', 'finance_rpt.rpt_tblorderlineuniquefuture', 'finance_rpt.rpt_tblqueuemismatch', 'finance_rpt.rpt_tbltwopcttolerance', 'finance_dm.fact_agg_cg', 'finance_dm.fact_fms_sales', 'finance_dm.fact_retail_noloc_wly', 'finance_dm.fact_retail_sellin', 'finance_dm.fact_retail_sellthru_invt', 'finance_dm.fact_retail_weekly', 'finance_dm.edm_dim_company_cur', 'finance_dm.epm_md_assetid', 'finance_dm.epm_vendor', 'finance_dm.fact_epic_financials_bu_vw', 'finance_dm.fact_epm_keymetrics_dly', 'finance_dm.fact_epm_keymetrics_dly_all', 'finance_dm.fact_order_line', 'finance_dm.fact_revenue_cogs', 'finance_dm.fact_revenue_consol', 'finance_dm.fact_revenue_rs', 'finance_dm.fact_revnu_exp_bal_cons', 'finance_dm.fact_sale_line', 'finance_dwh.dw_revstream', 'finance_dwh.dw_revstream_trx_item_map', 'finance_dwh.dw_sbl_order_item', 'finance_dm.dim_coa_company_hier', 'finance_dm.dim_coa_department_hier', 'finance_dm.dim_coa_ntrl_acct_hier', 'finance_dm.dim_date']
    
    initialLineage=spark.sql(sql_query.format("','".join(list_of_start_tables)))

    max_level = 10
    process_multiple_tables(list_of_start_tables, max_level)
    
