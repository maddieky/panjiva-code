global data "../../intermediate_dta_files"

/*
import delimited $data/full_imports_for_decomp.csv, bindquote(strict) clear

split(qdate), parse("-")
destring(qdate1), replace
destring(qdate2), replace

drop qdate
gen qdate = yq(qdate1, qdate2)
format qdate %tq

order qdate 
drop qdate1 qdate2 

sort qdate

save $data/full_import_decomp.dta, replace
*/

**Step 1: Create two datasets: quarter = t, quarter = t + 1
  
foreach k in 239  235  231 227 {
	
	forvalues j = 1(1)6  {
		local i = `k'+`j'
	
		use $data/full_import_decomp.dta, clear
		
		keep if qdate==`i' | qdate==`k'

		order qdate
		ren (vol_teu weight_tot shpmtorigin shppanjivaid) (volumeteu weightkg country shipper)

		** Quarter = t 
		preserve
		keep if qdate==`i'
		ren (shpt volumeteu weightkg) (quantity_t TEU_t weight_t)
		save $data/imports_d2_q.dta, replace
		
		gen quarter = `i'
		collapse (sum) quantity_t TEU_t weight_t, by(quarter)
		ren (quantity_t TEU_t weight_t) (tot_quantity_t tot_TEU_t tot_weight_t)
		save $data/imports_d2_totcheck_q.dta, replace
		
		restore
		
		** Quarter = t-1
		keep if qdate==`k'
		ren (shpt volumeteu weightkg) (quantity_tm1 TEU_tm1 weight_tm1)
		save $data/imports_d1_q.dta, replace
		
		gen quarter = `i'
		collapse (sum) quantity_tm1 TEU_tm1 weight_tm1, by(quarter)
		ren (quantity_tm1 TEU_tm1 weight_tm1) (tot_quantity_tm1 tot_TEU_tm1 tot_weight_tm1)
		save $data/imports_d1_totcheck_q.dta, replace
		

		** Step 3: Join "month = t" and "month=t-1" datasets by supplier, consignee, product, country
		** Save matches as separate dataset 
		** Save "t+1" observations that do not match and "t" observations that do not match

		use $data/imports_d1_q.dta, clear

		merge 1:1 conpanjivaid shipper country using $data/imports_d2_q.dta
		
		** Redacted Consignee -- Intensive
		
		preserve 
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		keep if conpanjivaid == .		
		gen type = ""
			replace type = "Redacted Consignee: Intensive margin (decrease)" if TEU_t< TEU_tm1
			replace type = "Redacted Consignee: Intensive margin (increase)" if TEU_t > TEU_tm1
			replace type = "Redacted Consignee: Intensive margin (same)" if TEU_t == TEU_tm1
		collapse (sum) TEU_t TEU_tm1, by(quarter type)
		save $data/imports_d1_d2_supplier_country_m_red_q.dta, replace
		restore
		
		preserve 
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		keep if conpanjivaid == .
		gen type = ""
			replace type = "Redacted Consignee: Intensive margin (decrease)" if quantity_t < quantity_tm1
			replace type = "Redacted Consignee: Intensive margin (increase)" if quantity_t > quantity_tm1
			replace type = "Redacted Consignee: Intensive margin (same)" if quantity_t == quantity_tm1
		collapse (sum) quantity_t quantity_tm1, by(quarter type)
		save $data/imports_d1_d2_supplier_country_m_red_q_q.dta, replace
		restore
		
		preserve 
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		keep if conpanjivaid == .
		gen type = ""
			replace type = "Redacted Consignee: Intensive margin (decrease)" if weight_t < weight_tm1
			replace type = "Redacted Consignee: Intensive margin (increase)" if weight_t > weight_tm1
			replace type = "Redacted Consignee: Intensive margin (same)" if weight_t == weight_tm1
		collapse (sum) weight_t weight_tm1, by(quarter type)
		save $data/imports_d1_d2_supplier_country_m_red_w_q.dta, replace
		
		
		use $data/imports_d1_d2_supplier_country_m_red_q.dta, clear
		merge 1:1 quarter type using $data/imports_d1_d2_supplier_country_m_red_q_q.dta, nogen
		merge 1:1 quarter type using $data/imports_d1_d2_supplier_country_m_red_w_q.dta, nogen

		save $data/imports_d1_d2_supplier_country_m_red_q.dta, replace
		erase $data/imports_d1_d2_supplier_country_m_red_q_q.dta
		erase $data/imports_d1_d2_supplier_country_m_red_w_q.dta
		restore
		
		** Intensive margin (increase and decrease)
		preserve 
		keep if _merge == 3
		drop _merge
		
		gen quarter =`i'
		drop if conpanjivaid == .
		gen type = ""
			replace type = "Intensive margin (decrease)" if TEU_t < TEU_tm1
			replace type = "Intensive margin (increase)" if TEU_t > TEU_tm1
			replace type = "Intensive margin (same)" if TEU_t == TEU_tm1
		collapse (sum) TEU_t TEU_tm1, by(quarter type)
		save $data/imports_d1_d2_supplier_country_m_type_q.dta, replace
		restore
		
		preserve 
		keep if _merge == 3
		drop _merge
		
		gen quarter =`i'
		drop if conpanjivaid == .
		gen type = ""
			replace type = "Intensive margin (decrease)" if quantity_t < quantity_tm1
			replace type = "Intensive margin (increase)" if quantity_t > quantity_tm1
			replace type = "Intensive margin (same)" if quantity_t == quantity_tm1
		collapse (sum) quantity_t quantity_tm1, by(quarter type)
		save $data/imports_d1_d2_supplier_country_m_type_q_q.dta, replace
		restore
		
		preserve 
		keep if _merge == 3
		drop _merge
		
		gen quarter =`i'
		drop if conpanjivaid == .
		gen type = ""
			replace type = "Intensive margin (decrease)" if weight_t < weight_tm1
			replace type = "Intensive margin (increase)" if weight_t > weight_tm1
			replace type = "Intensive margin (same)" if weight_t == weight_tm1
		collapse (sum) weight_t weight_tm1, by(quarter type)
		save $data/imports_d1_d2_supplier_country_m_type_w_q.dta, replace
		
		use $data/imports_d1_d2_supplier_country_m_type_q.dta, clear
		merge 1:1 quarter type using $data/imports_d1_d2_supplier_country_m_type_q_q.dta, nogen
		merge 1:1 quarter type using $data/imports_d1_d2_supplier_country_m_type_w_q.dta, nogen
		save $data/imports_d1_d2_supplier_country_m_type_q.dta, replace
		
		erase $data/imports_d1_d2_supplier_country_m_type_q_q.dta
		erase $data/imports_d1_d2_supplier_country_m_type_w_q.dta
		restore


		**Save for next step
		**---------
		preserve
		keep if _merge == 1
		save $data/imports_d1_sup_nm_temp_q.dta, replace
		drop _merge shipper 
		collapse(sum) TEU_tm1 quantity_tm1 weight_tm1, by (conpanjivaid country)
		save $data/imports_d1_sup_nm_q.dta, replace
		
		use $data/imports_d1_sup_nm_temp_q.dta, clear
		drop _merge 
		collapse(sum) TEU_tm1 quantity_tm1 weight_tm1, by (conpanjivaid shipper country)
		save $data/imports_d1_country_nm_step3_q.dta, replace
		restore

		preserve 
		keep if _merge == 2
		save $data/imports_d2_sup_nm_temp_q.dta, replace
		drop _merge shipper 
		collapse(sum) TEU_t quantity_t weight_t, by (conpanjivaid country)
		save $data/imports_d2_sup_nm_q.dta, replace 
		use $data/imports_d2_sup_nm_temp_q.dta, clear
		drop _merge 
		collapse(sum) TEU_t quantity_t weight_t, by (conpanjivaid shipper country)
		save $data/imports_d2_country_nm_step3_q.dta, replace
		restore 
		**---------


		**----------------------------------------------------------------------
		** Step 4: Join two resulting datasets by consignee, product, country (no supplier)
		** Save matches as separate dataset (supplier switching margin - same country)
		** Keep "t+1" and "t" observations that do not match
		**----------------------------------------------------------------------
		
		** Now that we're merging without supplier, we need to collapse on consignee:
		use $data/imports_d1_sup_nm_q.dta, clear
		merge 1:1 conpanjivaid country using $data/imports_d2_sup_nm_q.dta

		**Same Country, but different supplier
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Same country, diff supplier (decrease)" if TEU_t < TEU_tm1
			replace type = "Same country, diff supplier (increase)" if TEU_t > TEU_tm1
			replace type = "Same country, diff supplier (same)" if TEU_t == TEU_tm1
		collapse (sum) TEU_t TEU_tm1, by(quarter type)
		save $data/imports_d1_d2_con_m_q.dta, replace
		restore
		
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Same country, diff supplier (decrease)" if quantity_t < quantity_tm1
			replace type = "Same country, diff supplier (increase)" if quantity_t > quantity_tm1
			replace type = "Same country, diff supplier (same)" if quantity_t == quantity_tm1
		collapse (sum) quantity_t quantity_tm1, by(quarter type)
		save $data/imports_d1_d2_con_m_q_q.dta, replace
		restore

		
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Same country, diff supplier (decrease)" if weight_t < weight_tm1
			replace type = "Same country, diff supplier (increase)" if weight_t > weight_tm1
			replace type = "Same country, diff supplier (same)" if weight_t == weight_tm1
		collapse (sum) weight_t weight_tm1, by(quarter type)
		save $data/imports_d1_d2_con_m_w_q.dta, replace
		
		use $data/imports_d1_d2_con_m_q.dta, replace
		merge 1:1 quarter type using $data/imports_d1_d2_con_m_q_q.dta, nogen
		merge 1:1 quarter type using $data/imports_d1_d2_con_m_w_q.dta, nogen

		save $data/imports_d1_d2_con_m_q.dta, replace
		erase $data/imports_d1_d2_con_m_q_q.dta
		erase $data/imports_d1_d2_con_m_w_q.dta
		restore


		**Save for next step
		**---------
		preserve
		keep if _merge == 1
		drop _merge
		drop quantity* weight* TEU*
		merge 1:m conpanjivaid country using $data/imports_d1_country_nm_step3_q.dta
		**_m==1: 
		**_m==2: are observations that matched in step 4 above
		**_m==3: are observations that did not match in step 4 above
		keep if _m==3
		collapse(sum) TEU_tm1 quantity_tm1 weight_tm1, by (conpanjivaid shipper)
		save $data/imports_d1_con_nm_q.dta, replace
		restore

		preserve 
		keep if _merge == 2
		drop _merge
		drop quantity* weight* TEU*
		merge 1:m conpanjivaid country using $data/imports_d2_country_nm_step3_q.dta
		**_m==1: 
		**_m==2: are observations that matched in step 4 above
		**_m==3: are observations that did not match in step 4 above
		keep if _m==3
		collapse(sum) TEU_t quantity_t weight_t, by (conpanjivaid shipper)
		save $data/imports_d2_con_nm_q.dta, replace 
		restore 
		**---------
		
		
		**----------------------------------------------------------------------
		** Step 5: Join resulting datasets by consignee, supplier (no country)
		**----------------------------------------------------------------------
		use $data/imports_d1_con_nm_q.dta, clear
		merge 1:1 conpanjivaid shipper using $data/imports_d2_con_nm_q.dta
		
		**Same Supplier, but different Country
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Same supplier, diff country (decrease)" if TEU_t < TEU_tm1
			replace type = "Same supplier, diff country (increase)" if TEU_t > TEU_tm1
			replace type = "Same supplier, diff country (same)" if TEU_t == TEU_tm1
		collapse (sum) TEU_t TEU_tm1, by(quarter type)
		save $data/imports_d1_d2_supp_m_q.dta, replace
		restore
		
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Same supplier, diff country (decrease)" if quantity_t < quantity_tm1
			replace type = "Same supplier, diff country (increase)" if quantity_t > quantity_tm1
			replace type = "Same supplier, diff country (same)" if quantity_t == quantity_tm1
		collapse (sum) quantity_t quantity_tm1, by(quarter type)
		save $data/imports_d1_d2_supp_m_q_q.dta, replace
		restore
		
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Same supplier, diff country (decrease)" if weight_t < weight_tm1
			replace type = "Same supplier, diff country (increase)" if weight_t > weight_tm1
			replace type = "Same supplier, diff country (same)" if weight_t == weight_tm1
		collapse (sum) weight_t weight_tm1, by(quarter type)
		save $data/imports_d1_d2_supp_m_w_q.dta, replace
		
		
		use $data/imports_d1_d2_supp_m_q.dta, clear
		merge 1:1 quarter type using $data/imports_d1_d2_supp_m_q_q.dta, nogen
		merge 1:1 quarter type using $data/imports_d1_d2_supp_m_w_q.dta, nogen
		save $data/imports_d1_d2_supp_m_q.dta, replace
		
		erase $data/imports_d1_d2_supp_m_q_q.dta
		erase $data/imports_d1_d2_supp_m_w_q.dta
		restore
		
		**Save for next step
		**---------
		preserve
		keep if _merge == 1
		drop _merge
		keep conpanjivaid weight_tm1 TEU_tm1 quantity_tm1
		collapse (sum) weight_tm1 TEU_tm1 quantity_tm1, by(conpanjivaid)
		save $data/imports_d1_just_consignee_nm_q.dta, replace 
		restore

		preserve 
		keep if _merge == 2
		drop _merge
		keep conpanjivaid weight_t TEU_t quantity_t
		collapse (sum) weight_t TEU_t quantity_t, by(conpanjivaid)
		save $data/imports_d2_just_consignee_nm_q.dta, replace 
		restore
		**---------
		
		**----------------------------------------------------------------------
		** Step 6: Join resulting datasets by consignee only
		**----------------------------------------------------------------------
		use $data/imports_d1_just_consignee_nm_q.dta, clear 
		merge 1:1 conpanjivaid using $data/imports_d2_just_consignee_nm_q.dta 
		
		**Different Supplier, AND different Country
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Diff supplier, diff country (decrease)" if TEU_t < TEU_tm1
			replace type = "Diff supplier, diff country (increase)" if TEU_t > TEU_tm1
			replace type = "Diff supplier, diff country (same)" if TEU_t == TEU_tm1
		collapse (sum) TEU_t TEU_tm1, by(quarter type)
		save $data/imports_d1_d2_diff_m_TEU_q.dta, replace
		restore
		
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Diff supplier, diff country (decrease)" if weight_t < weight_tm1
			replace type = "Diff supplier, diff country (increase)" if weight_t > weight_tm1
			replace type = "Diff supplier, diff country (same)" if weight_t == weight_tm1
		collapse (sum) weight_t weight_tm1, by(quarter type)
		save $data/imports_d1_d2_diff_m_weight_q.dta, replace
		restore
		
		preserve
		keep if _merge == 3
		drop _merge
		gen quarter = `i'
		gen type = ""
			replace type = "Diff supplier, diff country (decrease)" if quantity_t < quantity_tm1
			replace type = "Diff supplier, diff country (increase)" if quantity_t > quantity_tm1
			replace type = "Diff supplier, diff country (same)" if quantity_t == quantity_tm1
		collapse (sum) quantity_t quantity_tm1, by(quarter type)
		merge 1:1 quarter type using $data/imports_d1_d2_diff_m_weight_q.dta, nogen
		merge 1:1 quarter type using $data/imports_d1_d2_diff_m_TEU_q.dta, nogen
			
		save $data/imports_d1_d2_diff_m_q.dta, replace
		erase $data/imports_d1_d2_diff_m_weight_q.dta
		erase $data/imports_d1_d2_diff_m_TEU_q.dta
		restore
		
		**Consignee Death
		**---------
		preserve
		keep if _merge == 1
		drop _merge
		gen quarter = `i'
		collapse (sum) quantity_tm1 TEU_tm1 weight_tm1, by(quarter)
		gen type = "Consignee Death"
		save $data/imports_d1_consignee_death_q.dta, replace 
		restore

		**Consignee Birth
		preserve 
		keep if _merge == 2
		drop _merge
		gen quarter = `i'
		collapse (sum) quantity_t TEU_t weight_t, by(quarter)
		gen type = "Consignee Birth"
		save $data/imports_d1_consignee_birth_q.dta, replace 
		restore
		**---------
		
		
		
		**----------------------------------------------------------------------
		** Step 7: Append Everything Together, Save
		**----------------------------------------------------------------------
		use $data/imports_d1_d2_supplier_country_m_red_q.dta, clear
		append using $data/imports_d1_d2_supplier_country_m_type_q.dta
		append using $data/imports_d1_d2_con_m_q.dta
		append using $data/imports_d1_d2_supp_m_q.dta
		append using $data/imports_d1_d2_diff_m_q.dta
		append using $data/imports_d1_consignee_death_q.dta
		append using $data/imports_d1_consignee_birth_q.dta
		save $data/imports_d1_d2_con_results_q.dta, replace
		
		**Merge in total checks
		merge m:1 quarter using $data/imports_d1_totcheck_q.dta
		drop _m
		merge m:1 quarter using $data/imports_d2_totcheck_q.dta
		drop _m

		** Check with Aaron: With the redacted intensive margin, sums don't add up (which makes sense, 
		** I think because we're subsetting), so need to assert without quantity from redact.

		egen totquantcheck = total(quantity_t)
		assert totquantcheck==tot_quantity_t
		
		gen tot_TEU_change = tot_TEU_t - tot_TEU_tm1
		gen tot_weight_change = tot_weight_t - tot_weight_tm1
		gen tot_quantity_change = tot_quantity_t - tot_quantity_tm1
		foreach qtyp in quantity TEU weight {
		
			replace `qtyp'_tm1 = 0 if type=="Consignee Birth"
			replace `qtyp'_t = 0 if type=="Consignee Death"
		
			gen `qtyp'_change = (`qtyp'_t - `qtyp'_tm1)
			gen contrib_`qtyp' = `qtyp'_change/tot_`qtyp'_change
			
		}
	
		rename (quantity_t TEU_t weight_t) (quantity_t`j' TEU_t`j' weight_t`j')
		rename (quantity_tm1 TEU_tm1 weight_tm1) (quantity_tm1_`j' TEU_tm1_`j' weight_tm1_`j')
		rename totquantcheck totquantcheck`j'
		
		rename (tot_quantity_t tot_TEU_t tot_weight_t) (tot_quantity_t`j' tot_TEU_t`j' tot_weight_t`j')
		rename (tot_quantity_change tot_TEU_change tot_weight_change) (tot_quantity_change`j' tot_TEU_change`j' tot_weight_change`j')
		rename (TEU_change weight_change quantity_change) (TEU_change`j' weight_change`j' quantity_change`j')
		replace quarter = `k'
		save $data/imports_decomposition_q_dyn_`k'_`j'.dta, replace
	}
	
}

foreach k in 239 235 231 227 {
	use $data/imports_decomposition_q_dyn_`k'_1.dta
	
	forvalues j = 2(1)6  {
		merge 1:1 quarter type using $data/imports_decomposition_q_dyn_`k'_`j'.dta
		drop _m
		erase $data/imports_decomposition_q_dyn_`k'_`j'.dta
	}

	save $data/imports_decomposition_q_dyn_`k'.dta, replace
	erase $data/imports_decomposition_q_dyn_`k'_1.dta
	
}

use $data/imports_decomposition_q_dyn_239.dta, clear
append using $data/imports_decomposition_q_dyn_235.dta
append using $data/imports_decomposition_q_dyn_231.dta
append using $data/imports_decomposition_q_dyn_227.dta
save $data/imports_decomposition_q_dyn.dta, replace
