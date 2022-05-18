global datadir "../../intermediate_dta_files"
*cd $datadir

use $datadir/knownimo_vessel_crosswalk.dta, clear

	bys primary vesselimo: gen dup = _N 
	drop dup
	
	*In the instances where the vessel has the same name, but different ship IMO
	bys primary: gen dup2 = _N
	
	preserve
	keep if dup2 > 1
	drop dup2 uniqueid dup 
	gen multiple_imo = 1
	save $datadir/multiple_imos_to_same_name.dta, replace // Use this data later on to generate crosswalk to figure out what the typical route is 
	** The resulting crosswalk is built in $datadir/generate_port_crosswalk_for_multiple_IMOs_same_name.do
	** $datadir/to_merge_unknownimos_multiplenames
	restore
	
	preserve
	drop if dup2 > 1
	drop dup2
	save $datadir/formerge_knownimo_nodup.dta, replace 
	restore 
	
drop dup2
		
save $datadir/formerge_knownimo.dta, replace 

import delimited $datadir/port_analysis_data_daily_from_2012.csv, encoding(ISO-8859-2) clear 

** Minor de-dup: there are 188 "dups" based on ports, vessel, ID, date, shipments, weight, and volume -- drop these **
bys portofunlading portoflading vessel vesselvoyageid date volumeteu weightkg shipments: gen dup = _N
tab dup 
gen tag = 1 if dup == 2 & vesselim == "None"

drop if dup == 2 & tag == 1 

drop dup tag 

preserve
drop if vesselimo == "None"
merge m:1 vesselimo using $datadir/formerge_knownimo.dta 
	drop _merge 
	
save $datadir/final_knownimos_toappend1.dta, replace // FILE TO APPEND  million obs 3,470,455 (these are just standardized names)
restore 

keep if vesselimo == "None" // 2,527,672 obs total

foreach char in "#" "$" "&" "'" "(" ")" "+" "," "-" ":" ";" "?" "[" "]" "_" "." {
replace vessel = subinstr(vessel, "`char'", "", .)
}

replace vessel = stritrim(vessel)

gen primary = vessel  
drop vesselimo 

** This will only capture unique names found in the known IMO crosswalk to unique names in the unknown IMO data ** 

merge m:1 primary using $datadir/formerge_knownimo_nodup.dta //  2,045,883 merged; 481, 789 remaining

snapshot save // 2

preserve
keep if _merge == 3
drop _merge uniqueid 
save $datadir/append_toknown2.dta, replace // FILE TO APPEND
restore 

** Work on remaining 481,789
** There are a good chunk of observations where it looks like the vesselimo was made the vessel name.
** Extract these cases and merge onto known IMO crosswalk **

snapshot restore 2

keep if _merge == 1
drop _merge uniqueid

gen length = length(primary)
gen tag = 1 if regexm(primary, "[A-Z]")

br if tag == . & length == 7

** Export separately to merge and then drop **
keep if tag == . & length == 7
drop vesselimo-altname3
gen vesselimo = primary 
drop vessel tag length primary

merge m:1 vesselimo using $datadir/formerge_knownimo.dta // 7,358 merged 

preserve
keep if _merge == 1 // 200 obs
save $datadir/knownimo_novesselname_200.dta, replace

gen obs = 1
collapse(sum) obs, by (vesselimo)
gsort -obs
save $datadir/knownimo_no_vesselname_tomerge.dta, replace
restore 

keep if _merge == 3
drop _merge uniqueid 

save $datadir/unknown_tomerge_toappend.dta, replace // FILE TO APPEND 

snapshot restore 2

** Continue identifying unknown IMOs

keep if _merge == 1
drop _merge uniqueid

gen length = length(primary)
gen tag = 1 if regexm(primary, "[A-Z]")

preserve 
gen name_ind = 1 if length <= 7
keep if tag == . & length == 7 | name_ind == 1 
save $datadir/likely_to_drop.dta, replace
restore


drop if tag == . & length == 7
gen name_ind = 1 if length <= 7

keep if name_ind == . // 22,500 unique names left to merge
drop vesselimo - name_ind

save $datadir/full_unmerged_420k.dta, replace //420k observations left 

** 34k unique names left to merge (representing 420k obs)  
gen obs = 1 
collapse(sum) obs, by (primary)

save $datadir/final_final_unmatchednames.dta, replace

** Did this process once, do not repeat because was confirmed by hand **

/*
gen id = _n

reclink2 primary using $datadir/knownimo_vessel_crosswalk.dta, idmaster(id) idusing(uniqueid) gen(score) manytoone

** Manually confirm 2k vessel names
save $datadir/markedup_reclink_confirmed.dta, replace

** Need to drop duplicates (same name to two IMOs) for merge
use $datadir/markedup_reclink_confirmed.dta, clear
collapse(sum) keep, by (primary Uprimary vesselimo altname*)
bys primary: gen dup = _N
drop if dup > 1
drop dup
save $datadir/final_reclink_tomerge.dta, replace
*/

use $datadir/full_unmerged_420k.dta, clear
merge m:1 primary using $datadir/final_reclink_tomerge.dta // 53,501 matched 

preserve
keep if _merge == 3
drop _merge keep 
drop primary
ren Uprimary primary
save $datadir/unknown_to_appendtofinal.dta, replace    //  FILE TO APPEND
restore

keep if _merge == 1 // 367k
drop _merge keep 

drop Uprimary-altname3

save $datadir/remaining_360_full.dta, replace

gen obs = 1
collapse(sum) obs, by (primary)

gen tag = 1 if obs >= 100
bys tag: egen total = sum(obs)

export delimited $datadir/final_missing_vessel_names.csv, replace

**** Merge the manually found imos back in ****
use $datadir/remaining_360_full.dta, clear
drop primary

preserve 
import delimited $datadir/final_missing_vessel_names_updated.csv, clear
keep if vesselimo ~= ""
drop obs max alternative current v8
ren (primary vesselimo) (vessel imo) 
tempfile t
save `t'
restore 

merge m:1 vessel using `t' // gets us 95k variables back

** Last 270k variables missing **
preserve
keep if _merge == 1
save $datadir/remaining_last_270k.dta, replace
restore

keep if _merge == 3
ren imo vesselimo
drop _merge 

preserve
use $datadir/final_knownimos_toappend1.dta, clear // Represents stardardized KNOWN IMOs (2,004,271)
	keep vesselimo primary altname*
	duplicates drop
tempfile t
save `t', replace
restore

merge m:1 vesselimo using `t'

drop if _merge == 2

replace primary = vessel if _merge == 1
drop _merge 

save $datadir/prev_unknown_imo_95k.dta, replace

**** APPEND ITERATIVE MERGES BACK TOGETHER (yields 5.67m of 6mil) *****
use $datadir/final_knownimos_toappend1.dta, clear      // Represents all known IMOs
	drop uniqueid
append using $datadir/append_toknown2.dta              // Merge of primary vessel names (without dup names, diff IMOs)
append using $datadir/unknown_tomerge_toappend.dta     // Represents known IMOs that were saved as vessel names
append using $datadir/unknown_to_appendtofinal.dta     // Represents results of fuzzy link 
append using $datadir/prev_unknown_imo_95k.dta         // Represents manually entered IMOs

duplicates drop

drop tag

** Drop duplicates 
bys portofunlading portoflading vesselvoyageid date volumeteu weightkg shipments: gen dup = _N
drop if dup > 1
drop dup

** Confirm total standardization of "primary" vessel name **
** This tells us that there are 6 ships that are still associated with 2 names ** 
** Manually fix in main - use google to decide which the ship actually is based off IMO **
** Looks like BoL also confuses MSC and Maersk -- all dups (22 total) have that issue **

preserve
gen obs = 1
collapse(sum) obs, by (primary vesselimo)
bys vesselimo: gen dup = _N
tab dup
br if dup == 2
restore 

replace primary = "MSC ATLANTIC" if vesselimo == "8913447"
replace primary = "MSC BAHAMAS" if vesselimo == "9118288"
replace primary = "MAERSK KINGSTON" if vesselimo == "9244934"
replace primary = "MAERSK KALAMATA" if vesselimo == "9244946"
replace primary = "MSC CAROLINA" if vesselimo == "9295397"
replace primary = "MSC ALTAIR" if vesselimo == "9465277"

replace primary = "MSC SONIA" if vesselimo == "7111999"
replace primary = "MSC LAURENCE" if vesselimo == "7510420"
replace primary = "MSC BRIANNA" if vesselimo == "9103685"
replace primary = "SEALAND MERCURY" if vesselimo == "9106194"
replace primary = "GREEN DALE" if vesselimo == "9181376"
replace primary = "YM SINGAPORE" if vesselimo == "9256224"



** Final dup check **
bys portofunlading portoflading date volumeteu weightkg shipments: gen dup = _N
drop if dup > 1 
drop dup

save $datadir/final_standardized_vessels_2012.dta, replace // Represents 5.6 of 6m obs 

** Re-standardize anything after the big append 
use $datadir/final_standardized_vessels_2012.dta, clear

preserve
gen obs = 1 
collapse(sum) obs, by (vesselimo primary)

bys vesselimo: gen dup = _N
keep if dup > 1

gsort vesselim -obs
bys vesselimo: gen alt = primary[_n+1] if dup == 2
drop if dup == 2 & alt == ""
drop dup obs

ren primary new_name
drop alt

tempfile t
save `t', replace
restore 

merge m:1 vesselimo using `t'

gen tag = 1 if primary != new_name & _merge == 3
replace primary = new_name if tag == 1 & _merge == 3

drop new_name _merge tag alt*

save $datadir/final_standardized_vessels_2012.dta, replace


/*
******* Observations that never merged -- for future, explore adding back in unmerged without adding duplicates *******

use $datadir/v2_final_standardized_vessels.dta, clear // 3.389
append using $datadir/remaining_last_105k.dta  // 105327   = 3494900
append using $datadir/likely_to_drop.dta //  33000 -- total of 3.528.196 -- full dataset has 3.524.388

drop dup-name_ind

duplicates drop

bys portofunlading portoflading vesselvoyageid date volumeteu weightkg shipments: gen dup = _N
tab dup 
br if dup > 1

gen tag = 1 if length(vessel) == 7 & ~regexm(vessel, "[A-Z]")
drop if tag == 1 // 3.524.168
drop dup

bys portofunlading portoflading vesselvoyageid date volumeteu weightkg shipments: gen dup = _N


