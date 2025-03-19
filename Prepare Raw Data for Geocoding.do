// cd "D:\OneDrive\OneDrive - Objectvision\VU\Projects\NVM Prijsindex\"
cd "C:\Users\Jip\OneDrive - Objectvision\VU\Projects\NVM Prijsindex\" //ovsrv06

use Brondata\nvm19852021_raw.dta

global exportdate = 20250307

g geocode_id = _n

rename obj_* *
rename hid_* *
rename *, lower

save Temp\nvm19852021_raw_rename.dta

keep geocode_id straatnaam huisnummer huisnummertoevoeging postcode woonplaats

export delimited using "C:\GeoDMS\SourceData\BAG\xy_source\NVM_1985_2021_raw.csv", delimiter(";") replace

//GEOCODE IN GEODMS

import delimited Brondata\nvm19852021_geocoded_slim.csv, clear 
import delimited "C:\GeoDMS\LocalData\BAG\Geocode\resultaat.csv", stringcols(17) clear 
save Brondata\nvm19852021_geocoded_slim_${exportdate}.dta, replace

use Temp\nvm19852021_raw_rename.dta, clear

merge 1:1 geocode_id using Brondata\nvm19852021_geocoded_slim_${exportdate}.dta 

drop count var bag_postcode bag_straatnaam bag_huisnummer bag_huisletter bag_toevoeging bag_woonplaatsnaam v25 _merge

compress
save Brondata\nvm19852021_geocoded_${exportdate}.dta, replace 


use Brondata\nvm19852021_geocoded_${exportdate}.dta, clear


keep datum_afm transactieprijs m2 gem_id 
