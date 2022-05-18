clear 
set more off

**============================================================================================
**Aaron Flaaen 
**July 8, 2021
**This File Explores Vessel-Port Extract of Panjiva
**============================================================================================

/**************************************************************************************
*Structure of File
**Input Data:   final_standardized_vessels_2012.dta
**Output Data: data_ec_numdays.csv, data_lalb_numdays.csv, data_lalb_reroutes.csv
**************************************************************************************/

global datadir "../../intermediate_dta_files"

**Part 1: Explore Data -- Calculate Time in Between Port-Vessel Visit
**------------------------------------------------------------------------------

**See final_vessel_clean.do for a bunch of cleaning of the vessel name and vessel imo
use $datadir/final_standardized_vessels_2012.dta, clear

**Replace original vessel name with cleaned version
replace vessel = primary

rename date arrivaldate
gen date = date(arrivaldate,"YMD")
format date %td		
		
gen year = yofd(date)
		
destring volumeteu, replace ignore(NA)

**You can really get a sense of the complicated nature of these routes by looking at this vessel
sort vessel date portoflading
br if vessel=="NYK RIGEL"
				
collapse (sum) shipments weightkg volumeteu, by(vessel vesselimo portofunlading date)
egen vesselgroup = group(vessel vesselimo)
		
**Multiple ports of unlading on same day by a given vessel? There are a lot of them
bys vesselgroup date: gen dup = _N
tab dup

**Looking at some of these, there is NO WAY the vessel could unload at these ports in same day
**Some of these are like Seattle and Tacoma, and New York and Newark.  Either a mis-report or could it be possible?
br if dup>1

**A big chunk look to be errors in reporting (almost all the records are for one portofunlading, and just one at other port)
bys vesselgroup date: egen tot_TEU = total(volumeteu)
gen share = volumeteu / tot_TEU



**Part 2: Clean port routes in our extract of Panjiva data
**------------------------------------------------------------------------------
* Combine LA/Long Beach, Seattle/Tacoma, Newark/NY
replace portofunlading="LA/LB" if regexm(portofunlading,"Los Angeles")==1 | regexm(portofunlading,"Long Beach")==1
replace portofunlading="Seattle-Tacoma" if regexm(portofunlading,"Seattle")==1 | regexm(portofunlading,"Tacoma")==1
replace portofunlading="Newark/NY" if regexm(portofunlading,"Newark")==1 | regexm(portofunlading,"New York")==1
		
* Create a variable, dominantport, that indicates if that shipment went to a port of unlading that was the dominant port of unlading (over 80% of all shipments) for that vessel on a given day
bysort date vessel portofunlading: gen numports = _N
bysort date vessel: gen totalports = _N
gen shareports = numports/totalports
gen dominantport = 0
replace dominantport = 1 if shareports > 0.8
drop *ports

* Next, we assume all shipments that aren't at a dominant port are errors

* If there is not dominant port (e.g., dominantport never = 1)  for a given vessel on a day: we will drop all these obs
bysort date vessel: egen numdominantport = total(dominantport)
drop if numdominantport == 0
drop numdominantport

* If there is a dominant port (e.g., dominant port does = 1 at least once) for a given vessel on a day: replace obs with the non-dominant port (dominantport = 0) with the dominant port of unlading
gen portofunlading2 = portofunlading
sort date vessel dominantport
by date vessel: replace portofunlading2 = portofunlading2[_N] if dominantport == 0

* Check: There are no more shipments from the same vessel on the same day to multiple ports
list if vessel == vessel & date == date & portofunlading2 != portofunlading2
drop portofunlading
ren portofunlading2 portofunlading

keep volumeteu weightkg shipments vessel vesselimo date portofunlading
save $datadir/cleaned_port_data_interm.dta, replace


**Part 3: Further changes to vessel name and IMO
**------------------------------------------------------------------------------
use $datadir/cleaned_port_data_interm.dta, clear
rename shipments shpt
bys vessel date portofunlading: gen dup = _N

**Take Maximum by TEU, then shipments, then weight
bys vessel date portofunlading: egen totTEU = total(volumeteu)
gen share = volumeteu/totTEU
bys vessel date portofunlading: egen maxshare = max(share)
drop if dup==1
drop if dup>1 & maxshare~=share
drop dup maxshare share totTEU 
bys vessel date portofunlading: gen dup = _N
bys vessel date portofunlading: egen totweight = total(weightkg)
gen share = weightkg/totweight
bys vessel date portofunlading: egen maxshare = max(share)
drop if dup>1 & maxshare~=share
drop dup maxshare share totweight 
bys vessel date portofunlading: gen dup = _N
bys vessel date portofunlading: egen totshpt = total(shpt)
gen share = shpt/totshpt
bys vessel date portofunlading: egen maxshare = max(share)
drop if dup>1 & maxshare~=share
drop dup maxshare share totshpt
bys vessel date portofunlading: gen dup = _N
bys vessel date portofunlading: gen dup2 = _n
drop if dup==2 & dup2==1
drop dup dup2
bys vessel date portofunlading: gen dup = _N
assert dup == 1
rename vesselimo vesselimo_new
keep vesselimo_new vessel portofunlading date
save $datadir/temp_more_vessel_fixes2.dta, replace

**Merge back in
use $datadir/cleaned_port_data_interm.dta, clear
rename shipments shpt
keep date volumeteu portofunlading weightkg vessel vesselimo shpt
	
destring volumeteu, replace ignore(NA)
		
gen qdate = qofd(date)

format qdate %tq
		
collapse (sum) shpt volumeteu weightkg, by (date portofunlading vessel vesselimo)

merge m:1 date portofunlading vessel using $datadir/temp_more_vessel_fixes2.dta
replace vesselimo = vesselimo_new if _m==3 & vesselimo~=vesselimo_new
drop vesselimo_new _m
collapse (sum) shpt volumeteu weightkg, by (date portofunlading vessel vesselimo)

save $datadir/cleaned_port_data.dta, replace

**Part 4: Calculate fraction of TEUs from same vessel, and redirection
**------------------------------------------------------------------------------
use $datadir/cleaned_port_data.dta, clear
format date %td		

egen vessel_port = group(vesselimo portofunlading)

**This chart shows the problem
twoway bar volumeteu date if vesselimo=="9465265" & portofunlading=="LA/LB" & volumeteu>2000 & (yofd(date)==2019 | yofd(date)==2018), yaxis(1) || ///
bar volumeteu date if vesselimo=="9465265" & portofunlading=="LA/LB" & volumeteu<2000 & (yofd(date)==2019 | yofd(date)==2018), yaxis(2) ///
legend(region(color(none)) ring(0) position(12) linegap(1)) legend(order(1 2)) legend(rows(2) cols(1) size(small))  ///
 legend(lab(1 "Date Has > 2000 TEU, left"  ) lab(2 "Date Has <2000 TEU, right")) title("Unladings at LA/LB by Maersk Vega, by TEU groupings", lcolor(black)) ///
graphregion(color(white) margin(l=0 r=0 t=4 b=0)) xtitle("") ytitle("Number of TEUs", axis(1)) ytitle("Number of TEUs", axis(2)) ///
plotregion(margin(l=0 b=0 t=0 r=0) style(none))	yscale(range(0 14000) axis(1)) yscale(range(0 500) axis(2))

**Use our cleaned data to identify some estimate of vessel capacity (imperfect)
use $datadir/cleaned_port_data.dta, clear
collapse (max) volumeteu, by(vesselimo vessel)
rename volumeteu capacity_teu

**Remove vessels that are clearly not container ships
**May want to play around with this. (CAPACITY CUTOFF)
drop if capacity_teu<200
kdensity capacity_teu
save $datadir/capacity_vessels_container.dta, replace

**Create dataset of only these "container" vessels
use $datadir/cleaned_port_data.dta, clear
merge m:1 vesselimo vessel using $datadir/capacity_vessels_container.dta
keep if _m==3
drop _m
format date %td		
save $datadir/cleaned_port_container_data.dta, replace

**Loop over Fourth quarters in 2019, 2018, 2017, 2016
foreach k in 239 235 231 227 {
	
	forvalues n= 1(-1)0 {
		local m = `k'-`n'
		use $datadir/cleaned_port_container_data.dta, clear
		gen qdate = qofd(date)
		format qdate %tq
		keep if qdate==`m' 
		
		collapse (sum) shpt volumeteu weightkg, by (qdate portofunlading vesselimo)
		
		save $datadir/cleaned_port_container_`m'_temp.dta, replace
	}
	use $datadir/cleaned_port_container_`k'_temp.dta, replace
	forvalues n= 1(-1)1 {
		local m = `k'-`n'
		merge 1:1 vesselimo portofunlading using $datadir/cleaned_port_container_`m'_temp.dta
		keep if _m==3
		drop _m
	}
	keep vesselimo portofunlading
	duplicates drop
	save $datadir/temp_consistent_vessel_list.dta, replace
	
	
	**Look at subsequent 6 quarters
	forvalues j = 1(1)6  {
		
		local i = `k'+`j'
		
		
		***Quarter t
		use $datadir/cleaned_port_container_data.dta, clear
		
		gen qdate = qofd(date)
		format qdate %tq
		keep if qdate==`k'
		
		**Merge with consistent vessel list
		merge m:1 vesselimo portofunlading using $datadir/temp_consistent_vessel_list.dta
		keep if _m==3
		
		gen utilization = volumeteu/capacity_teu
		
		**May want to play around with this. (UTILIZATION CUTOFF)
		drop if utilization<0.1
		
		
		format date %td		
		keep qdate volumeteu portofunlading weightkg vessel vesselimo shpt capacity_teu
		
		collapse (sum) shpt volumeteu weightkg (firstnm) capacity_teu, by (qdate portofunlading vesselimo)
			
		
		ren (shpt volumeteu weightkg) (quantity_t TEU_t weight_t)
		drop qdate
		save $datadir/red_t1.dta, replace
		
		
		***Quarter t+`j'
		use $datadir/cleaned_port_container_data.dta, clear
		gen qdate = qofd(date)
		format qdate %tq
		
		keep if qdate==`i'
		
		
		gen utilization = volumeteu/capacity_teu
		**May want to play around with this. (UTILIZATION CUTOFF)
		drop if utilization<0.1
		
		
		keep qdate volumeteu portofunlading weightkg vessel vesselimo shpt
		
		collapse (sum) shpt volumeteu weightkg, by (qdate portofunlading vesselimo)
		
		ren (shpt volumeteu weightkg) (quantity_tp1 TEU_tp1 weight_tp1)
		drop qdate
		save $datadir/red_t2.dta, replace
		
				
		**Step: Merge together with same routes
		*merge 1:1 portofunlading vessel vesselimo using $datadir/red_t1.dta, 
		merge 1:1 portofunlading vesselimo using $datadir/red_t1.dta, 
		
		preserve
		keep if _m==3 
		collapse (sum) quantity_* TEU_* weight_*, by( portofunlading)
		gen qdate_base = `k'
		gen qdate_forward = `i'
		save $datadir/red_merged_`k'_`i'.dta, replace
		restore
		
		preserve
		keep if _m==1
		drop quantity_t TEU_t weight_t _m
		rename portofunlading portofunlading_new
		save $datadir/red_unmerged_`i'.dta, replace
		restore
		
		keep if _m==2
		drop quantity_tp1 TEU_tp1 weight_tp1 _m
		rename portofunlading portofunlading_old
		
		**Step: Merge together now with same routes except for port of unlading
		joinby vesselimo using $datadir/red_unmerged_`i'.dta, unmatched(both)
		
		replace portofunlading_new="none" if _m==1
		foreach var of varlist TEU_tp1 quantity_tp1 weight_tp1 {
			replace `var' = 0 if _m==1	
		}
		
		replace portofunlading_old = "none" if _m==2 & portofunlading_old==""
		foreach var of varlist TEU_t quantity_t weight_t {
			replace `var' = 0 if _m==2 & `var'==.	
		}
	
		
		foreach var of varlist TEU_t weight_t quantity_t {
			bys vessel vesselimo portofunlading_new: egen tot_`var' = total(`var')
			gen share`var' = `var'/tot_`var'
			replace `var'p1 = `var'p1*share`var' if share`var'~=.
			replace `var'p1 = 0 if share`var'==.
		}
		
		bys vesselimo portofunlading_old: gen dup = _N
		bys vesselimo portofunlading_old: gen dup2 = _n
		drop dup dup2
		
		collapse (sum) quantity_* TEU_* weight_*, by(portofunlading_old portofunlading_new)
		gen qdate_base = `k'
		gen qdate_forward = `i'
		save $datadir/red_newport_`k'_`i'.dta, replace
		
		erase $datadir/red_unmerged_`i'.dta
		
	}
	
	local i = `k'+1
	use $datadir/red_newport_`k'_`i'.dta, clear
	forvalues j = 2(1)6 {
		local i = `k'+`j'
		append using $datadir/red_newport_`k'_`i'.dta
	}
	save $datadir/red_newport_`k'.dta, replace
	
	local i = `k'+1
	use $datadir/red_merged_`k'_`i'.dta, clear
	forvalues j = 2(1)6 {
		local i = `k'+`j'
		append using $datadir/red_merged_`k'_`i'.dta
	}
	save $datadir/red_merged_`k'.dta, replace
	
	
	forvalues j = 1(1)6 {
		local i = `k'+`j'
		erase $datadir/red_newport_`k'_`i'.dta
		erase $datadir/red_merged_`k'_`i'.dta
	}
	
}

erase $datadir/red_t1.dta
erase $datadir/red_t2.dta


**All Ports
**------------------------
use $datadir/red_merged_239.dta, clear
rename portofunlading portofunlading_old
gen portofunlading_new = portofunlading_old
append using $datadir/red_newport_239.dta

order portofunlading_old portofunlading_new
bys qdate_base qdate_forward portofunlading_new: egen tot_TEU_tp1 = total(TEU_tp1)
gen TEU_share = TEU_tp1/tot_TEU_tp1
drop if portofunlading_old==portofunlading_new

gen qdate = qdate_forward - qdate_base

gen port_type = 1 if regexm(portofunlading_old,"LA/LB")==1
replace port_type = 2 if regexm(portofunlading_old,"Port of Oakland")==1
replace port_type = 3 if regexm(portofunlading_old,"Savannah")==1
replace port_type = 4 if regexm(portofunlading_old,"Newark")==1
replace port_type = 5 if regexm(portofunlading_old,"Norfolk")==1
replace port_type = 6 if port_type==.

collapse (sum) TEU_tp1 TEU_share, by(qdate_base qdate port_type portofunlading_new)

drop if qdate==6

keep qdate TEU_tp1 TEU_share port_type qdate_base portofunlading

save $datadir/newports_239_ports.dta, replace

foreach qd in 235 231 227 {

	use $datadir/red_merged_`qd'.dta, clear
	rename portofunlading portofunlading_old
	gen portofunlading_new = portofunlading_old
	append using $datadir/red_newport_`qd'.dta

	order portofunlading_old portofunlading_new
	bys qdate_base qdate_forward portofunlading_new: egen tot_TEU_tp1 = total(TEU_tp1)
	gen TEU_share = TEU_tp1/tot_TEU_tp1
	drop if portofunlading_old==portofunlading_new

	gen qdate = qdate_forward - qdate_base

	gen port_type = 1 if regexm(portofunlading_old,"LA/LB")==1
	replace port_type = 2 if regexm(portofunlading_old,"Port of Oakland")==1
	replace port_type = 3 if regexm(portofunlading_old,"Savannah")==1
	replace port_type = 4 if regexm(portofunlading_old,"Newark")==1
	replace port_type = 5 if regexm(portofunlading_old,"Norfolk")==1
	replace port_type = 6 if port_type==.

	collapse (sum) TEU_tp1 TEU_share, by(qdate_base qdate port_type portofunlading_new)

	**At some point we'll have full June data, so we can drop this.
	drop if qdate==6

	keep qdate TEU_tp1 TEU_share port_type qdate_base portofunlading_new

	save $datadir/newports_`qd'_ports.dta, replace
}

use $datadir/newports_235_ports.dta, clear
append using $datadir/newports_231_ports.dta
collapse (mean) TEU_tp1 TEU_share, by(qdate port_type portofunlading_new)
rename TEU_tp1 TEU_oldavg
rename TEU_share TEU_share_oldavg
merge 1:1 qdate port_type portofunlading_new using $datadir/newports_239_ports.dta

gen TEU_diff = TEU_tp1 - TEU_oldavg
gen TEU_share_diff  = TEU_share - TEU_share_oldavg

replace TEU_share_diff = TEU_share_diff*100


**All Ports - Outbound perspective 
**------------------------
use $datadir/red_merged_239.dta, clear
rename portofunlading portofunlading_old
gen portofunlading_new = portofunlading_old
append using $datadir/red_newport_239.dta

order portofunlading_old portofunlading_new

* Generate denominator variable (sum of all obs where "new" port is LA/LB) + replace missing values with this new total sum
bys qdate_base qdate_forward: egen denom_TEU_tp1 = total(TEU_tp1) if portofunlading_new=="LA/LB"
bys qdate_base qdate_forward: egen denom_TEU_tp2 = max(denom_TEU_tp1)
drop denom_TEU_tp1
rename denom_TEU_tp2 denom_TEU_tp1

* Generate numerator variable (sum of all obs where "old" port is LA/LB but "new" port is a different port)
bys qdate_base qdate_forward: gen num_TEU_tp1 = TEU_tp1 if portofunlading_old=="LA/LB" & portofunlading_new ~= "LA/LB"

* Create TEU share (i.e. num/denom)
gen TEU_share = num_TEU_tp1/denom_TEU_tp1
drop if portofunlading_old==portofunlading_new

gen qdate = qdate_forward - qdate_base

gen port_type = 1 if regexm(portofunlading_old,"LA/LB")==1
replace port_type = 2 if regexm(portofunlading_old,"Port of Oakland")==1
replace port_type = 3 if regexm(portofunlading_old,"Savannah")==1
replace port_type = 4 if regexm(portofunlading_old,"Newark")==1
replace port_type = 5 if regexm(portofunlading_old,"Norfolk")==1
replace port_type = 6 if port_type==.

collapse (sum) TEU_tp1 TEU_share, by(qdate_base qdate port_type portofunlading_new)


**Create weighted average of Major East Coast Ports
replace portofunlading_new = "Major East Coast" if regexm(portofunlading_new, "Norfolk") | regexm(portofunlading_new, "Savannah") | regexm(portofunlading_new, "Charleston, South") | regexm(portofunlading_new, "Newark")
bys portofunlading_new qdate port_type: egen sumTEU_tp1 = total(TEU_tp1)
gen weight = TEU_tp1/sumTEU_tp1
replace TEU_share = TEU_share*weight

collapse (sum) TEU_tp1 TEU_share, by(qdate_base qdate port_type portofunlading_new)

*drop if qdate==6

keep qdate TEU_tp1 TEU_share port_type qdate_base portofunlading

save $datadir/newports_239_ports_outbound.dta, replace

* Now create the same share of rerouted outbound TEUs from LA for 2017, 2018, and 2019
foreach qd in 235 231 227 {

	use $datadir/red_merged_`qd'.dta, clear
	rename portofunlading portofunlading_old
	gen portofunlading_new = portofunlading_old
	append using $datadir/red_newport_`qd'.dta


	* Generate denominator variable (sum of all obs where "new" port is LA/LB) + replace missing values with this new total sum
	bys qdate_base qdate_forward: egen denom_TEU_tp1 = total(TEU_tp1) if portofunlading_new=="LA/LB"
	bys qdate_base qdate_forward: egen denom_TEU_tp2 = max(denom_TEU_tp1)
	drop denom_TEU_tp1
	rename denom_TEU_tp2 denom_TEU_tp1

	* Generate numerator variable (sum of all obs where "old" port is LA/LB but "new" port is a different port)
	bys qdate_base qdate_forward: gen num_TEU_tp1 = TEU_tp1 if portofunlading_old=="LA/LB" & portofunlading_new ~= "LA/LB"

	* Create TEU share (i.e. num/denom)
	gen TEU_share = num_TEU_tp1/denom_TEU_tp1
	drop if portofunlading_old==portofunlading_new

	gen qdate = qdate_forward - qdate_base

	gen port_type = 1 if regexm(portofunlading_old,"LA/LB")==1
	replace port_type = 2 if regexm(portofunlading_old,"Port of Oakland")==1
	replace port_type = 3 if regexm(portofunlading_old,"Savannah")==1
	replace port_type = 4 if regexm(portofunlading_old,"Newark")==1
	replace port_type = 5 if regexm(portofunlading_old,"Norfolk")==1
	replace port_type = 6 if port_type==.

	collapse (sum) TEU_tp1 TEU_share, by(qdate_base qdate port_type portofunlading_new)

	**Create weighted-average of Major East Coast Ports
	replace portofunlading_new = "Major East Coast" if regexm(portofunlading_new, "Norfolk") | regexm(portofunlading_new, "Savannah") | regexm(portofunlading_new, "Charleston, South") | regexm(portofunlading_new, "Newark")
	bys portofunlading_new qdate port_type: egen sumTEU_tp1 = total(TEU_tp1)
	gen weight = TEU_tp1/sumTEU_tp1
	replace TEU_share = TEU_share*weight
	
	collapse (sum) TEU_tp1 TEU_share, by(qdate_base qdate port_type portofunlading_new)
	
	**At some point we'll have full June data, so we can drop this.
	*drop if qdate==6

	keep qdate TEU_tp1 TEU_share port_type qdate_base portofunlading_new

	save $datadir/newports_`qd'_ports_outbound.dta, replace
}

* Create average rerouted TEUs per quarter using 2017, 2018, and 2019 data 
use $datadir/newports_235_ports_outbound.dta, clear
append using $datadir/newports_231_ports_outbound.dta
append using $datadir/newports_227_ports_outbound.dta
collapse (mean) TEU_tp1 TEU_share, by(qdate port_type portofunlading_new)
rename TEU_tp1 TEU_oldavg
rename TEU_share TEU_share_oldavg

* Now merge in 2020 data and calculate difference from average
merge 1:1 qdate port_type portofunlading_new using $datadir/newports_239_ports_outbound.dta

gen TEU_diff = TEU_tp1 - TEU_oldavg
gen TEU_share_diff  = TEU_share - TEU_share_oldavg

replace TEU_share_diff = TEU_share_diff*100

keep if regexm(portofunlading_new, "Seattle")==1 | regexm(portofunlading_new, "Oakland")==1 | regexm(portofunlading_new, "East Coast")
keep if port_type == 1
format qdate_base %tq
replace portofunlading_new="ec" if regexm(portofunlading_new, "East Coast")==1
replace portofunlading_new="oakland" if regexm(portofunlading_new, "Oakland")==1
replace portofunlading_new="seattle" if regexm(portofunlading_new, "Seattle")==1
rename qdate date
keep portofunlading_new TEU_share_diff date
export delimited "../../data_for_paper/data_lalb_reroutes.csv", replace

**Part 5: Calculate time in between vessel unloadings
**------------------------------------------------------------------------------
**Now by Port of Unlading

**Use our notion of capacity to eliminate vessels that are not container ships
use $datadir/cleaned_port_data.dta, clear
format date %td	
merge m:1 vessel vesselimo using $datadir/capacity_vessels_container.dta
keep if _m==3

**Merge in Capacity and Drop if Utilization of Capacity is too low
drop if volumeteu==0
gen utilization = volumeteu/capacity_teu

drop if utilization<0.1

egen vessel_port = group(vesselimo portofunlading)

tsset vessel_port date, daily
gen days = date - date[_n-1]
replace days = . if vessel_port~=vessel_port[_n-1]

**Clean up the days variable

**Step 1: Replace date if vessel shows trips less than 5 days
gen close_date = date[_n-1] if days<5 & vessel_port==vessel_port[_n-1]

**Multiple close by
*gen flag = 0
*replace flag = 1 if close_date~=. & days<5 & vessel_port==vessel_port[_n-1] & close_date[_n-1]~=.
replace close_date = close_date[_n-1] if days<5 & vessel_port==vessel_port[_n-1] & close_date~=. & close_date[_n-1]~=.

replace date = close_date if close_date~=. & vessel_port==vessel_port[_n-1]

collapse (sum) volumeteu weightkg shpt, by(vessel_port portofunlading date)
tsset vessel_port date, daily

gen days = date - date[_n-1]
replace days = . if vessel_port~=vessel_port[_n-1]

**Step 2: Winsorize Top
sum days, detail

bys portofunlading: egen topdays = pctile(days), p(95)
*replace days = topdays if days>topdays & days~=.
drop if days>topdays & days~=.
sum days, detail
**--------

gen monthvar = mofd(date)

bys monthvar portofunlading: egen sumvolumeteu = total(volumeteu)
bys monthvar portofunlading: egen sumkg = total(weightkg)

gen tw_days = days*volumeteu/sumvolumeteu
gen kw_days = days*weightkg/sumkg

preserve
gen year = yofd(dofm(monthvar))
keep if year > 2012 & year < 2018
collapse (median) med_days_2013_2017 =days, by(portofunlading)
tempfile t
save `t'
restore 

collapse (mean) volumeteu weightkg days (median) med_days =days (p25) p25_days = days (p75) p75_days=days (sum) tw_days kw_days, by(monthvar portofunlading)

gen year = yofd(dofm(monthvar))

merge m:1 portofunlading using `t', nogen

sort portofunlading year month
label variable med_days_2013_2017 "2013-2017 average"

* LA/LB port
preserve
keep if portofunlading == "LA/LB" 
keep monthvar med_days med_days_2013_2017
format monthvar %tm
export delimited "../../data_for_paper/data_lalb_numdays.csv", replace
restore

* Aggregate East Coast ports
* Create weighted average of med_days by TEU for top East Coast ports
keep if regexm(portofunlading,"Charleston")==1 | regexm(portofunlading,"Norfolk")==1 | regexm(portofunlading,"Newark")==1 | regexm(portofunlading,"Savannah")==1

bys monthvar: egen total_TEU = total(volumeteu)
gen weight = volumeteu/total_TEU
gen weighted_med_days = med_days*weight
collapse (sum) weighted_med_days (mean) med_days_2013_2017, by (monthvar year)
format monthvar %tm
export delimited "../../data_for_paper/data_ec_numdays.csv", replace
