clear
cd "C:\Users\JipClaassens\OneDrive - Objectvision\VU\Projects\NVM Prijsindex\" //ovsrv08

global filename = "NVM_1985_2023"
global exportdate = 20250520

use Brondata/${filename}.dta, clear


use EnhancedData/${filename}_${exportdate}_cleaned.dta, clear



g geocode_id = _n

rename obj_* *
rename hid_* *
rename *, lower

keep geocode_id straatnaam huisnummer huisnummertoevoeging postcode woonplaats

export delimited using E:/SourceData/BAG/xy_source/${filename}_${exportdate}_cleaned_slim.csv, delimiter(";") replace

//GEOCODE IN GEODMS
import delimited Brondata/${filename}_${exportdate}_cleaned_slim_Geocoded.csv, clear
// import delimited Brondata/${filename}_${exportdate}_slim_geocoded.csv, clear

save Temp/${filename}_cleaned_slim_geocoded.dta, replace
// save Temp/${filename}_slim_geocoded.dta, replace

use EnhancedData/${filename}_${exportdate}_cleaned.dta, clear
// use Brondata/${filename}.dta, clear

g geocode_id = _n
// rename obj_* *
// rename hid_* *
// rename *, lower

merge 1:1 geocode_id using Temp/${filename}_cleaned_slim_geocoded.dta 
// merge 1:1 geocode_id using Temp/${filename}_slim_geocoded.dta 

drop count var bag_postcode bag_straatnaam bag_huisnummer bag_huisletter bag_toevoeging bag_woonplaatsnaam _merge v2*

compress
save Brondata/${filename}_cleaned_geocoded_${exportdate}.dta, replace 
// save Brondata/${filename}_geocoded_${exportdate}.dta, replace 


// use Brondata\nvm19852021_geocoded_${exportdate}.dta, clear
// keep datum_afm transactieprijs m2 gem_id 






