snapshot erase _all
set more off 

** Step 1 of 2: Vessel cleaning **

global datadir "../../intermediate_dta_files"

import delimited $datadir/port_analysis_data_daily_from_2012.csv, encoding(ISO-8859-2) clear 

** Generate a crosswalk of KNOWN IMOs to standardize the "primary" name
gen obs = 1 
collapse(sum) obs, by (vesselimo vessel)
sort vesselimo

foreach char in "#" "$" "&" "'" "(" ")" "+" "," "-" ":" ";" "?" "[" "]" "_" "." {
replace vessel = subinstr(vessel, "`char'", "", .)
}

replace vessel = stritrim(vessel)

** Making this crosswalk on KNOWN only 
drop if vesselimo == "None"
collapse(sum) obs, by (vesselimo vessel)

bys vesselimo: gen dup = _N
tab dup

bys vesselimo: egen max = max(obs)

** Primary name has the most observations per IMO
** STEP 1: VARIABLE NAMES IF OBSERVATIONS ARE THE SAME 
snapshot save // 1

snapshot restore 1
keep if dup > 1
gsort vesselimo -obs

gen secondary = ""
bys vesselimo: replace secondary = vessel[_n+1] 

gen third = "" 
bys vesselimo: replace third = vessel[_n+2] if dup == 3 | dup == 4

gen fourth = ""
bys vesselimo: replace fourth = vessel[_n+3] if dup == 3 | dup == 4

gsort vesselimo -obs 
by vesselimo: gen n = _n 

keep if n == 1

tempfile t
save `t', replace

snapshot restore 1

keep if dup == 1
append using `t'

drop obs dup max n 
gen primary = vessel
ren (secondary third fourth) (altname altname2 altname3)
order vesselimo vessel primary altname altname2 altname3

bys vesselimo: gen dup = _N
tab dup
drop dup
gen uniqueid = _n 


save $datadir/knownimo_vessel_crosswalk.dta, replace 
