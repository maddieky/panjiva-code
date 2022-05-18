global data "../../intermediate_dta_files"

use $data/imports_decomposition_q_dyn.dta, clear

keep quarter type  tot_TEU_t* tot_TEU_tm1 TEU_change* tot_TEU_change* TEU_*

gen cond_type = "Switching"
replace cond_type = "Net Extensive Margin" if type=="Consignee Birth" | type=="Consignee Death"
replace cond_type = "Intensive Margin" if inlist(type,"Intensive margin (decrease)","Intensive margin (increase)","Intensive margin (same)")
replace cond_type = "Redacted" if regexm(type,"Redacted")==1
collapse (sum) TEU_t* TEU_change* (firstnm) tot_TEU_t* tot_TEU_change*, by(cond_type quarter)

forvalues i = 1(1)6 {
	gen perc_change_t`i' = ((tot_TEU_t`i'/tot_TEU_tm1)-1)*100
	gen perc_change_contrib`i' = (TEU_change`i'/tot_TEU_change`i')*perc_change_t`i'
}

drop TEU_t*
reshape long TEU_change perc_change_contrib perc_change_t, i(quarter cond_type) j(qdate)
encode cond_type, gen(cond_type_var)
drop cond_type
reshape wide TEU_change perc_change_contrib perc_change_t, i(quarter qdate) j(cond_type_var)

**Do the Comparison on One Chart
gen quarteralt = 0
replace quarteralt = 1  if quarter==239

drop if quarter == 235 & (qdate == 5 | qdate == 6)

collapse (mean) perc_change_contrib*, by(quarteralt qdate)


 graph bar perc_change_contrib2 perc_change_contrib1 perc_change_contrib4 perc_change_contrib3, over(quarteralt, relabel(1 "16-18" 2 "19")) ///
over(qdate, relabel(1 "t+1" 2 "t+2" 3 "t+3" 4 "t+4" 5 "t+5")) stack ///
bar(1,color(red%70)) bar(2,color(blue%70)) bar(3,color(dkgreen%70)) bar(4,color(orange%70)) ///
legend(region(color(none)) ring(0) position(5) linegap(1)) legend(rows(2) cols(2) size(small))  ///
 legend(lab(1 "Net Extensive Margin"  ) lab(2 "Intensive Margin") lab(3 "Switching") lab(4 "Redacted")) /// 
 title("Decomposing Change in TEU relative to 4th Quarter", color(black)) ///
 ytitle("Change in TEU (relative to 4th-Quarter)") graphregion(color(white) margin(l=0 r=0 t=4 b=0)) 

 text(-32000 10 "2020-Q1", size(small)) ///
 text(-22000 33 "2020-Q2", size(small)) ///
 text(23000 53 "2020-Q3", size(small)) ///
 text(41000 75 "2020-Q4", size(small)) ///
 text(40000 96 "2021-Q1", size(small)) ///
 yscale(range(-40000 41500)) ///
 plotregion(margin(l=0 b=0 t=0 r=0) style(none)) 


reshape wide perc_change_contrib1 perc_change_contrib2 perc_change_contrib3 perc_change_contrib4, i(qdate) j(quarteralt) 
forvalues i = 1(1)4 {
	gen perc_change_contribchange`i' = perc_change_contrib`i'1 - perc_change_contrib`i'0
}
keep perc_change_contribchange* qdate 
reshape long perc_change_contribchange, i(qdate) j(typevar)


gen perc_change_diff1 = 0
gen perc_change_diff2 = perc_change_contribchange if typevar==2
bys qdate: egen minperc_change_diff2 = min(perc_change_diff2)
gen perc_change_diff3 = perc_change_contribchange + minperc_change_diff2 if typevar==4
bys qdate: egen minperc_change_diff3 = min(perc_change_diff3)
gen perc_change_diff4 = perc_change_contribchange + minperc_change_diff3 if typevar==1
bys qdate: egen minperc_change_diff4 = min(perc_change_diff4)
gen perc_change_diff5 = perc_change_contribchange + minperc_change_diff4 if typevar==3

**Depending on whether one margin is positive or not)
replace minperc_change_diff2 = 0 if qdate==1
replace perc_change_diff3 = perc_change_contribchange if typevar==4 & qdate==1
replace minperc_change_diff3 = 0 if qdate==1
replace perc_change_diff4 = minperc_change_diff3+perc_change_contribchange if typevar==1 & qdate==1
bys qdate: egen minperc_change_diff4alt = min(perc_change_diff4) if qdate==1
replace minperc_change_diff4 = minperc_change_diff4alt if qdate==1
replace perc_change_diff5 = minperc_change_diff4+perc_change_contribchange if typevar==3 & qdate==1

bys qdate: egen totperc_change = total(perc_change_contribchange)

preserve 
keep perc_change_diff1 perc_change_diff2 qdate typevar
keep if typevar == 2
ren perc_change_diff2 nem
keep qdate nem

tempfile nem
save `nem', replace
restore

preserve
keep qdate typevar minperc_change_diff2 perc_change_diff3
keep if typevar == 4
gen switch = perc_change_diff3 - minperc
keep qdate switch

tempfile switch
save `switch', replace
restore

preserve
keep qdate typevar minperc_change_diff3 perc_change_diff4
keep if typevar == 1
gen im = perc_chan - minperc
keep qdate im

tempfile im 
save `im', replace
restore

preserve
keep qdate typevar minperc_change_diff4 perc_change_diff5
keep if typevar == 3
gen red = perc - minperc
keep qdate red

tempfile red
save `red', replace
restore 

preserve
keep totperc_change qdate type
keep if typevar == 1
ren totperc im_line 
keep qdate im_line

tempfile imline
save `imline', replace
restore

use `nem', clear
merge 1:1 qdate using `switch', nogen
merge 1:1 qdate using `im', nogen
merge 1:1 qdate using `red', nogen
merge 1:1 qdate using `imline', nogen

ren (nem switch im red im_line) (line_nem line_switch line_im line_red line_im_line)
reshape long line_, i(qdate) j(type)str

gen mdate = "2020-01" if qdate == 1
replace mdate = "2020-04" if qdate ==2
replace mdate = "2020-07" if qdate == 3
replace mdate = "2020-10" if qdate == 4
replace mdate = "2021-01" if qdate == 5
replace mdate = "2021-04" if qdate == 6

gen date = qofd(dofm(monthly(mdate, "YM")))
format date %tq

drop qdate
ren date qdate
order qdate
drop mdate 

preserve
keep if type == "im_line"
drop type 
ren line_ im_line
export delimited "../../data_for_paper/data_decomp_line_for_R_allimports.csv", replace
restore 

drop if type == "im_line"
gen val = 1 if type == "red"
replace val = 2 if type == "im"
replace val = 3 if type == "switch"
replace val = 4 if type == "nem"
ren line_ pct_chg
sort qdate val

export delimited "../../data_for_paper/data_decomp_for_R_allimports.csv", replace
