clear
capture log close
// cd "D:\OneDrive\OneDrive - Objectvision\VU\Projects\NVM Prijsindex\"
cd "C:\Users\Jip\OneDrive - Objectvision\VU\Projects\NVM Prijsindex\" //ovsrv06
log using temp/nvmsetup.txt, text replace

global maxprice = 5000000
global minprice = 25000
global maxsize = 500
global minsize = 10
global maxpricesqm = 15000
global minpricesqm = 100
global maxrooms = 25
global new_filedate = 20240416
global exportdate = 20250307

**# PREP new data set

use Brondata/nvm19852022_raw_${new_filedate}.dta, clear
drop object_id pc4 bag_adresseerbaarobject_id bag_nad_id lat lon buurt_nr_2019 buurt_naam_2019 wijk_nr_2019 wijk_naam_2019 gem_nr_2019 gem_naam_2019 buurt_nr_2021 buurt_naam_2021 wijk_nr_2021 wijk_naam_2021 gem_nr_2021 gem_naam_2021 buurt_nr_2022 buurt_naam_2022 wijk_nr_2022 wijk_naam_2022 gem_nr_2022 gem_naam_2022 corop_nr corop_naam prov_nr prov_naam urbanisatiegraad mengvorm soortveiling veilingtype recreatiewoning bouwperiode gebruiksoppoverigeinpandigeruimt gebruiksoppgebouwgebbuitenr gebruiksoppexternebergruimte brutoinhoud  schuur koopconditie koopspecificatie transactieconditie transactiedetail oorspr_vraagkoopprijs oorspr_vraagkoopprijs_m2 laatste_vraagkoopprijs laatste_vraagkoopprijs_m2 transactieprijs_m2 proc_verschil_oorspr proc_verschil_laatst aanmelddatum looptijd datumtransport year soortwoning soortappartement woningtype nvm_cijfers

rename xcoord x
rename ycoord y
rename gebruiksoppervlaktewoonfunctie oppervlak
rename perceeloppervlakte perceel
rename aantalkamers nkamers
replace x = int(x)
replace y = int(y)
rename monument d_monument

// determine transaction year
gen trans_date = dofc(datumondertekeningakte)
format trans_date %td
g trans_year = year(trans_date)
g trans_month = month(trans_date)
g trans_day = day(trans_date)

g maintoutside = (onderhoud_buiten-1)*0.125 if onderhoud_buiten>0
g maintinside = (onderhoud_binnen-1)*0.125 if onderhoud_binnen>0
g onbibu = maintoutside+maintinside
g d_maintgood = 0
replace d_maintgood = 1 if onbibu>1.375

// nvm cijfersnr
// 2 = tussen
// 3 = schakel
// 4 = hoek
// 5 = 2o1
// 6 = vrij
// 7 = app onbek
// 8 = app < 45
// 9 = app 45-70
// 10 = app > 70

g d_apartment = 0
replace d_apartment = 1 if nvm_cijfersnr >= 7 
g d_terraced = 0
replace d_terraced = 1 if nvm_cijfersnr==2 | nvm_cijfersnr==3 | nvm_cijfersnr==4 
g d_semidetached = 0
replace d_semidetached = 1 if nvm_cijfersnr==5 
g d_detached = 0
replace d_detached = 1 if nvm_cijfersnr==6 


drop datumondertekeningakte trans_date onderhoud_binnen onderhoud_buiten onbibu maintoutside maintinside nvm_cijfersnr

save EnhancedData/NVM_${new_filedate}_cleaned_renamed.dta, replace


**# Prep old dataset

use Brondata/nvm19852021_geocoded_20220405.dta, clear

drop pc6code gem_id prov_id nvmreg_id afd_id categorie inhoud huisklasse soorthuis kenmerkwoning soortapp soortwoning oorsprvrkooppr laatstvrkooppr procverschil verkoopcond loopt datum_aanmelding nvmcijfers isnieuwbouw isbelegging status openportiek lift kwaliteit nverdiep vtrap zolder vlier praktijkr woonka nbalkon ndakkap ndakterras nkeuken nbijkeuk nwc nbadk parkeer inpandig tuinlig tuinafw isol verw ligcentr ligmooi ligdrukw erfpacht_tonen permanent ged_verhuurd kelder monumentaal geocode_id buurt_code wijk_code gemeente_code nvmafd_code niveau_code nummeraanduiding_id bag_type_woonpand

g maintoutside = (onbu-1)*0.125 if onbu>0
g maintinside = (onbi-1)*0.125 if onbi>0

replace m2 = woonopp if m2 <= 0
rename m2 oppervlak
rename monument d_monument

// determine transaction year
gen trans_date = dofc(datum_afm)
format trans_date %td
g trans_year = year(trans_date)
g trans_month = month(trans_date)
g trans_day = day(trans_date)

g d_apartment = 0
replace d_apartment = 1 if type==-1 | type==0 
g d_terraced = 0
replace d_terraced = 1 if type==1 | type==2 | type==3 
g d_semidetached = 0
replace d_semidetached = 1 if type==4 
g d_detached = 0
replace d_detached = 1 if type==5 

//bwper
// -1	Geen bouwjaar mogelijk (geen woning)
// 0	Onbekend, voor 1500 of na transactiejaar
// 1	1500-1905
// 2	1906-1930
// 3	1931-1944
// 4	1945-1959
// 5	1960-1970
// 6	1971-1980
// 7	1981-1990
// 8	1991-2000 
// 9	≥ 2001

g nvm_bouwjaar = .
replace nvm_bouwjaar = 1900 if bwper == 1
replace nvm_bouwjaar = 1920 if bwper == 2
replace nvm_bouwjaar = 1935 if bwper == 3
replace nvm_bouwjaar = 1950 if bwper == 4
replace nvm_bouwjaar = 1965 if bwper == 5
replace nvm_bouwjaar = 1975 if bwper == 6
replace nvm_bouwjaar = 1985 if bwper == 7
replace nvm_bouwjaar = 1995 if bwper == 8
replace nvm_bouwjaar = 2001 if bwper == 9

g bouwjaar = bag_bouwjaar
replace bouwjaar = nvm_bouwjaar if bouwjaar == .

g onbibu = maintoutside+maintinside
g d_maintgood = 0
replace d_maintgood = 1 if onbibu>1.375

drop bwper woonopp type datum_afmelding onbi onbu bag_bouwjaar maintoutside maintinside trans_date nvm_bouwjaar onbibu

**# Append new to old and harmonize variables

append using EnhancedData\NVM_${new_filedate}_cleaned_renamed.dta, gen(from2022) force
**# Bookmark #2

replace postcode = subinstr(postcode, " ", "", .)
replace nkamers = 1 if nkamers == 0

drop from2022

**# Clean up

// keep straatnaam huisnummer huisnummertoevoeging postcode woonplaats perceel oppervlak transactieprijs nkamers d_monument x y trans_year trans_month trans_day d_apartment d_terraced d_semidetached d_detached bouwjaar d_maintgood

/// Drop if price, size, or price per m2 exceed a certain value
drop if transactieprijs > $maxprice
drop if transactieprijs < $minprice
drop if oppervlak > $maxsize
drop if oppervlak < $minsize
g prijsm2 = transactiepr/oppervlak
drop if prijsm2 > $maxpricesqm
drop if prijsm2 < $minpricesqm
drop if nkamers > $maxrooms
replace nkamers = 1 if nkamers == 0
drop if x < 0 | x == . | x > 280000
drop if y < 300000 | y == . | y > 625000
drop prijsm2 

// Change number add to consistent format
replace huisnummertoev = upper(huisnummertoev)
// qui foreach letter in A B C D E F G H I J K L M N O P Q R S T U V W X Y Z II III IV V VI VII VIII IX BIS HS 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300 301 302 303 304 305 306 307 308 309 310 311 312 313 314 315 316 317 318 319 320 321 322 323 324 325 326 327 328 329 330 331 332 333 334 335 336 337 338 339 340 341 342 343 344 345 346 347 348 349 350 351 352 353 354 355 356 357 358 359 360 361 362 363 364 365 366 367 368 369 370 371 372 373 374 375 376 377 378 379 380 381 382 383 384 385 386 387 388 389 390 391 392 393 394 395 396 397 398 399 400 401 402 403 404 405 406 407 408 409 410 411 412 413 414 415 416 417 418 419 420 421 422 423 424 425 426 427 428 429 430 431 432 433 434 435 436 437 438 439 440 441 442 443 444 445 446 447 448 449 450 451 452 453 454 455 456 457 458 459 460 461 462 463 464 465 466 467 468 469 470 471 472 473 474 475 476 477 478 479 480 481 482 483 484 485 486 487 488 489 490 491 492 493 494 495 496 497 498 499 500 501 502 503 504 505 506 507 508 509 510 511 512 513 514 515 516 517 518 519 520 521 522 523 524 525 526 527 528 529 530 531 532 533 534 535 536 537 538 539 540 541 542 543 544 545 546 547 548 549 550 551 552 553 554 555 556 557 558 559 560 561 562 563 564 565 566 567 568 569 570 571 572 573 574 575 576 577 578 579 580 581 582 583 584 585 586 587 588 589 590 591 592 593 594 595 596 597 598 599 600 601 602 603 604 605 606 607 608 609 610 611 612 613 614 615 616 617 618 619 620 621 622 623 624 625 626 627 628 629 630 631 632 633 634 635 636 637 638 639 640 641 642 643 644 645 646 647 648 649 650 651 652 653 654 655 656 657 658 659 660 661 662 663 664 665 666 667 668 669 670 671 672 673 674 675 676 677 678 679 680 681 682 683 684 685 686 687 688 689 690 691 692 693 694 695 696 697 698 699 700 701 702 703 704 705 706 707 708 709 710 711 712 713 714 715 716 717 718 719 720 721 722 723 724 725 726 727 728 729 730 731 732 733 734 735 736 737 738 739 740 741 742 743 744 745 746 747 748 749 750 751 752 753 754 755 756 757 758 759 760 761 762 763 764 765 766 767 768 769 770 771 772 773 774 775 776 777 778 779 780 781 782 783 784 785 786 787 788 789 790 791 792 793 794 795 796 797 798 799 800 801 802 803 804 805 806 807 808 809 810 811 812 813 814 815 816 817 818 819 820 821 822 823 824 825 826 827 828 829 830 831 832 833 834 835 836 837 838 839 840 841 842 843 844 845 846 847 848 849 850 851 852 853 854 855 856 857 858 859 860 861 862 863 864 865 866 867 868 869 870 871 872 873 874 875 876 877 878 879 880 881 882 883 884 885 886 887 888 889 890 891 892 893 894 895 896 897 898 899 900 901 902 903 904 905 906 907 908 909 910 911 912 913 914 915 916 917 918 919 920 921 922 923 924 925 926 927 928 929 930 931 932 933 934 935 936 937 938 939 940 941 942 943 944 945 946 947 948 949 950 951 952 953 954 955 956 957 958 959 960 961 962 963 964 965 966 967 968 969 970 971 972 973 974 975 976 977 978 979 980 981 982 983 984 985 986 987 988 989 990 991 992 993 994 995 996 997 998 999 {
// 	replace huisnummertoev = "`letter'" if huisnummertoev=="-`letter'"
// }

replace huisnummertoev = subinstr(huisnummertoev,"-","",.)
replace huisnummertoev = subinstr(huisnummertoev,"!","",.)
replace huisnummertoev = "" if huisnummertoev=="ONG"
replace huisnummertoev = "" if huisnummertoev=="ONBEKEND"
replace huisnummertoev = subinstr(huisnummertoev,"+","",.)
replace huisnummertoev = subinstr(huisnummertoev,"/","-",.)
replace huisnummertoev = subinstr(huisnummertoev,".","",.)
replace huisnummertoev = subinstr(huisnummertoev,"NR","",.)
replace huisnummertoev = subinstr(huisnummertoev,"PP","",.)
replace huisnummertoev = subinstr(huisnummertoev,"HUIS","",.)
replace huisnummertoev = subinstr(huisnummertoev,"BG","",.)
replace huisnummertoev = subinstr(huisnummertoev,"pp","",.)
replace huisnummertoev = subinstr(huisnummertoev," ","",.)
replace huisnummertoev = subinstr(huisnummertoev,"#","",.)
replace huisnummertoev = subinstr(huisnummertoev,"ONG","",.)
replace huisnummertoev = subinstr(huisnummertoev,"&","",.)
replace huisnummertoev = subinstr(huisnummertoev,";","",.)
replace huisnummertoev = subinstr(huisnummertoev,"'","",.)
replace huisnummertoev = subinstr(huisnummertoev,"*","",.)
replace huisnummertoev = subinstr(huisnummertoev,"(","",.)
replace huisnummertoev = subinstr(huisnummertoev,")","",.)
replace huisnummertoev = subinstr(huisnummertoev,",","",.)
replace huisnummertoev = subinstr(huisnummertoev,"%","",.)
replace huisnummertoev = subinstr(huisnummertoev,"0","",.)
replace huisnummertoev = subinstr(huisnummertoev,"00","",.)
replace huisnummertoev = subinstr(huisnummertoev,"01","1",.)
replace huisnummertoev = subinstr(huisnummertoev,"02","2",.)
replace huisnummertoev = subinstr(huisnummertoev,"03","3",.)
replace huisnummertoev = subinstr(huisnummertoev,"04","4",.)
replace huisnummertoev = subinstr(huisnummertoev,"05","5",.)
replace huisnummertoev = subinstr(huisnummertoev,"06","6",.)
replace huisnummertoev = subinstr(huisnummertoev,"07","7",.)
replace huisnummertoev = subinstr(huisnummertoev,"08","8",.)
replace huisnummertoev = subinstr(huisnummertoev,"09","9",.)
replace huisnummertoev = subinstr(huisnummertoev,"03","3",.)
replace huisnummertoev = subinstr(huisnummertoev,"FLAT","",.)
replace huisnummertoev = subinstr(huisnummertoev,"FL","",.)
replace huisnummertoev = cond(huisnummertoev == "F", huisnummertoev, subinstr(huisnummertoev, "F", "", .))
replace huisnummertoev = subinstr(huisnummertoev,"\","",.)
replace huisnummertoev = subinstr(huisnummertoev,"]","",.)
replace huisnummertoev = subinstr(huisnummertoev,"^","",.)
replace huisnummertoev = subinstr(huisnummertoev,"_","",.)
replace huisnummertoev = subinstr(huisnummertoev,"`","",.)
replace huisnummertoev = subinstr(huisnummertoev,"{","",.)
replace huisnummertoev = subinstr(huisnummertoev,"|","",.)
replace huisnummertoev = subinstr(huisnummertoev,"ª","",.)
replace huisnummertoev = subinstr(huisnummertoev,"¬","",.)
replace huisnummertoev = subinstr(huisnummertoev,"²","",.)
replace huisnummertoev = subinstr(huisnummertoev,"À","A",.)
replace huisnummertoev = subinstr(huisnummertoev,"Ì","I",.)
replace huisnummertoev = subinstr(huisnummertoev,"Í","I",.)
replace huisnummertoev = subinstr(huisnummertoev,"È","E",.)
replace huisnummertoev = subinstr(huisnummertoev,"à","A",.)
replace huisnummertoev = subinstr(huisnummertoev,"Š","S",.)

replace postcode = subinstr(postcode,"!","",.)
replace postcode = "" if postcode=="0000XX"
replace postcode = "" if postcode=="0000XX"
replace postcode = "" if postcode=="1000"
replace postcode = "" if postcode=="0000"
replace postcode = "" if postcode=="0000AA"
replace postcode = "" if postcode=="0000AB"

replace huisnummer = . if huisnummer == 0

**# CLEAN DUPLICATES

g building_type_label = ""
replace building_type_label = "Terraced" if d_terraced == 1
replace building_type_label = "Semi-detached" if d_semidetached == 1
replace building_type_label = "Detached" if d_detached == 1
replace building_type_label = "Apartment" if d_apartment == 1
encode building_type_label, generate(building_type)

// add bouwjaar to record with no bouwjaar based on duplicate records
egen houseid0 = group(postcode huisnummer huisnummertoev building_type), missing
by houseid0, sort: egen bouwjaar_fromotherrecord = median(bouwjaar)
replace bouwjaar_fromotherrecord = int(bouwjaar_fromotherrecord)
replace bouwjaar = bouwjaar_fromotherrecord if bouwjaar == .
drop bouwjaar_fromotherrecord

// find duplicate records, first there could be slight differences in m2/rooms/bouwjaar that we ignore
by houseid0, sort: egen corr_m2 = mean(oppervlak)
by houseid0, sort: egen corr_nkamers = mean(nkamers)
by houseid0, sort: egen corr_bouwjaar = mean(bouwjaar)
replace corr_m2 = int((oppervlak-corr_m2-0.0001)/10) // 0 = no more than 5 m2 difference from mean
replace corr_nkamers = int((nkamers-corr_nkamers-0.0001)/5) // 0 = no more than 2.5 rooms difference from mean
replace corr_bouwjaar = int((bouwjaar-corr_bouwjaar-0.0001)/5) // 0 = no more than 2.5 jaar difference from mean
egen houseid1 = group(postcode huisnummer huisnummertoev building_type corr_m2 corr_nkamers corr_bouwjaar), missing
drop houseid0 corr_m2 corr_nkamers corr_bouwjaar

// remove the duplicates
egen duplicates_ID = group(houseid1 trans_year trans_month trans_day transactieprijs), missing
sort houseid1 trans_year trans_month trans_day transactieprijs
quietly by houseid1 trans_year trans_month trans_day transactieprijs:  gen dup = cond(_N==1,0,_n)
duplicates report duplicates_ID
drop if dup >= 2
drop duplicates_ID dup

// find repeat sales
duplicates tag houseid1, g(times)
replace times = times+1
duplicates tag houseid1 trans_year, g(yeartimes)
replace yeartimes = yeartimes+1
g random = 0
replace random = runiform() if times > 15 // change houseid if more than 15 times transacted in 30 year
replace random = runiform() if yeartimes > 1 // change houseid if more than 1 times transacted in one year
egen houseid = group(houseid1 random), missing
egen xyid = group(x y), missing

egen obsid = group(houseid trans_year)
duplicates report obsid

drop houseid1 times yeartimes random 

**# Enrich dataset

// g d_construnknown = 0
// replace d_construnknown = 1 if bouwjaar == .
// g d_constrlt1920 = 0
// replace d_constrlt1920 = 1 if bouwjaar <= 1919 & bouwjaar != .  
// g d_constr19201944 = 0 
// replace d_constr19201944 = 1 if bouwjaar >= 1920 & bouwjaar <= 1944
// g d_constr19451959 = 0 
// replace d_constr19451959 = 1 if bouwjaar >= 1945 & bouwjaar <= 1959
// g d_constr19601973 = 0 
// replace d_constr19601973 = 1 if bouwjaar >= 1960 & bouwjaar <= 1973
// g d_constr19741990 = 0 
// replace d_constr19741990 = 1 if bouwjaar >= 1974 & bouwjaar <= 1990
// g d_constr19911997 = 0 
// replace d_constr19911997 = 1 if bouwjaar >= 1991 & bouwjaar <= 1997
// g d_constrgt1997 = 0 
// replace d_constrgt1997 = 1 if bouwjaar >= 1998 & bouwjaar != .
//
// g construction_period_label = ""
// replace construction_period_label = "Unknown" if d_construnknown == 1
// replace construction_period_label = "Before 1920" if d_constrlt1920 == 1
// replace construction_period_label = "1920-1944" if d_constr19201944 == 1
// replace construction_period_label = "1945-1959" if d_constr19451959 == 1
// replace construction_period_label = "1960-1973" if d_constr19601973 == 1
// replace construction_period_label = "1974-1990" if d_constr19741990 == 1
// replace construction_period_label = "1991-1997" if d_constr19911997 == 1
// replace construction_period_label = "After 1997" if d_constrgt1997 == 1
// encode construction_period_label, generate(construction_period)


compress

order obsid xyid houseid trans_year trans_month trans_day perceel oppervlak transactieprijs bouwjaar straatnaam huisnummer huisnummertoevoeging postcode

replace straatnaam = subinstr(straatnaam,";","",.)
replace straatnaam = subinstr(straatnaam,"&","",.)
replace straatnaam = subinstr(straatnaam,"#","",.)
replace straatnaam = subinstr(straatnaam,".","",.)
replace straatnaam = subinstr(straatnaam,",","",.)
replace straatnaam = subinstr(straatnaam,"*","",.)
replace straatnaam = subinstr(straatnaam,"|","",.)

save Brondata/nvm19852022_geocoded_${exportdate}.dta, replace

export delimited using NVM_19852022_cleaned_${exportdate}.csv, delimiter(";") replace

