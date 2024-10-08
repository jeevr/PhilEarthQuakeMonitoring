select 
	date::date as date,
	time::time as time,
	latitude as lat,
	longitude as long,
	depth_km,
	magnitude,
	substring(details FROM position('EARTHQUAKE INFORMATION NO. :' IN details) + length('EARTHQUAKE INFORMATION NO. :') FOR 
              position(' PHIVOLCS Building' IN details) - position('EARTHQUAKE INFORMATION NO. :' IN details) - length('EARTHQUAKE INFORMATION NO. :'))::int as info_no,
	substring(details FROM position('Depth of Focus (Km) :' IN details) + length('Depth of Focus (Km) :') FOR 
	  	      position(' Origin' IN details) - position('Depth of Focus (Km) :' IN details) - length('Depth of Focus (Km) :'))::int as depth_of_focus_km,
	substring(details FROM position('Origin : ' IN details) + length('Origin : ') FOR 
	  	      position(' Magnitude' IN details) - position('Origin : ' IN details) - length('Origin : ')) as origin,
	substring(details FROM position('Expecting Damage : ' IN details) + length('Expecting Damage : ') FOR 
	  	      position(' Expecting Aftershocks' IN details) - position('Expecting Damage : ' IN details) - length('Expecting Damage : ')) as expecting_damage,
	substring(details FROM position('Expecting Aftershocks : ' IN details) + length('Expecting Aftershocks : ') FOR 
	  	      position(' Issued On' IN details) - position('Expecting Aftershocks : ' IN details) - length('Expecting Aftershocks : ')) as expecting_aftershock,
	hlink
from raw.tbldaily_earthquake_data



select *
from raw.tbldaily_earthquake_data