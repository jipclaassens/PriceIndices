clear
cd "C:\Users\JipClaassens\OneDrive - Objectvision\VU\Projects\NVM Prijsindex\" //ovsrv08

global filename = "NVM_1985_2023"
global importdate = 20250519
global exportdate = 20251024

use Brondata/${filename}.dta, clear
use EnhancedData/${filename}_${importdate}_cleaned.dta, clear



g geocode_id = _n

rename obj_* *
rename hid_* *
rename *, lower

keep geocode_id straatnaam huisnummer huisnummertoevoeging postcode woonplaats

export delimited using E:/SourceData/BAG/xy_source/${filename}_${exportdate}_cleaned_slim.csv, delimiter(";") replace

//=================
//GEOCODE IN GEODMS
//=================

import delimited EnhancedData/${filename}_${exportdate}_cleaned_slim_Geocoded.csv, clear

save Temp/${filename}_${exportdate}_cleaned_slim_geocoded.dta, replace

use EnhancedData/${filename}_${importdate}_cleaned.dta, clear

g geocode_id = _n
// rename obj_* *
// rename hid_* *
// rename *, lower

merge 1:1 geocode_id using Temp/${filename}_${exportdate}_cleaned_slim_geocoded.dta 

drop count var bag_postcode bag_straatnaam bag_huisnummer bag_huisletter bag_toevoeging bag_woonplaatsnaam _merge v2*

compress
save EnhancedData/${filename}_cleaned_geocoded_${exportdate}.dta, replace 



export delimited using EnhancedData/${filename}_${exportdate}_allvars_cleaned_geocoded.csv, delimiter(";") replace






