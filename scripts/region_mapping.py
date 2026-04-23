"""
Region Standardization Module for UK Job Market Intelligence Platform

This module provides functions to map UK cities to standardized ONS regions (NUTS1).
It extends the data cleaning pipeline to enable regional analysis.
"""

import logging
from typing import Dict, Optional

# Configure logging
logger = logging.getLogger(__name__)

# ONS NUTS1 Region Mapping Dictionary
# Maps UK cities to their corresponding ONS NUTS1 regions
REGION_MAP = {
    # London
    'london': 'London',
    'city of london': 'London',
    'westminster': 'London',
    'camden': 'London',
    'islington': 'London',
    'hackney': 'London',
    'tower hamlets': 'London',
    'greenwich': 'London',
    'lewisham': 'London',
    'southwark': 'London',
    'lambeth': 'London',
    'wandsworth': 'London',
    'hammersmith': 'London',
    'kensington': 'London',
    'chelsea': 'London',
    'richmond': 'London',
    'kingston': 'London',
    'merton': 'London',
    'sutton': 'London',
    'croydon': 'London',
    'bromley': 'London',
    'bexley': 'London',
    'havering': 'London',
    'barking': 'London',
    'redbridge': 'London',
    'newham': 'London',
    'waltham forest': 'London',
    'haringey': 'London',
    'enfield': 'London',
    'barnet': 'London',
    'harrow': 'London',
    'brent': 'London',
    'ealing': 'London',
    'hounslow': 'London',
    'hillingdon': 'London',
    
    # South East
    'brighton': 'South East',
    'hove': 'South East',
    'canterbury': 'South East',
    'dover': 'South East',
    'maidstone': 'South East',
    'tunbridge wells': 'South East',
    'ashford': 'South East',
    'chatham': 'South East',
    'gillingham': 'South East',
    'rochester': 'South East',
    'dartford': 'South East',
    'gravesend': 'South East',
    'sevenoaks': 'South East',
    'tonbridge': 'South East',
    'folkestone': 'South East',
    'margate': 'South East',
    'ramsgate': 'South East',
    'oxford': 'South East',
    'reading': 'South East',
    'slough': 'South East',
    'windsor': 'South East',
    'maidenhead': 'South East',
    'high wycombe': 'South East',
    'aylesbury': 'South East',
    'milton keynes': 'South East',
    'luton': 'South East',
    'watford': 'South East',
    'st albans': 'South East',
    'hemel hempstead': 'South East',
    'stevenage': 'South East',
    'welwyn garden city': 'South East',
    'hatfield': 'South East',
    'hertford': 'South East',
    'bishops stortford': 'South East',
    'harlow': 'South East',
    'chelmsford': 'South East',
    'colchester': 'South East',
    'southend': 'South East',
    'basildon': 'South East',
    'brentwood': 'South East',
    'romford': 'South East',
    'ilford': 'South East',
    'portsmouth': 'South East',
    'southampton': 'South East',
    'winchester': 'South East',
    'basingstoke': 'South East',
    'andover': 'South East',
    'aldershot': 'South East',
    'farnborough': 'South East',
    'guildford': 'South East',
    'woking': 'South East',
    'epsom': 'South East',
    'leatherhead': 'South East',
    'reigate': 'South East',
    'redhill': 'South East',
    'crawley': 'South East',
    'horsham': 'South East',
    'chichester': 'South East',
    'worthing': 'South East',
    'eastbourne': 'South East',
    'hastings': 'South East',
    
    # South West
    'bristol': 'South West',
    'bath': 'South West',
    'gloucester': 'South West',
    'cheltenham': 'South West',
    'swindon': 'South West',
    'exeter': 'South West',
    'plymouth': 'South West',
    'torquay': 'South West',
    'paignton': 'South West',
    'newton abbot': 'South West',
    'barnstaple': 'South West',
    'taunton': 'South West',
    'bridgwater': 'South West',
    'yeovil': 'South West',
    'weston-super-mare': 'South West',
    'bournemouth': 'South West',
    'poole': 'South West',
    'dorchester': 'South West',
    'weymouth': 'South West',
    'salisbury': 'South West',
    'truro': 'South West',
    'falmouth': 'South West',
    'penzance': 'South West',
    'st austell': 'South West',
    
    # West Midlands
    'birmingham': 'West Midlands',
    'coventry': 'West Midlands',
    'wolverhampton': 'West Midlands',
    'walsall': 'West Midlands',
    'west bromwich': 'West Midlands',
    'solihull': 'West Midlands',
    'dudley': 'West Midlands',
    'sutton coldfield': 'West Midlands',
    'stoke-on-trent': 'West Midlands',
    'stafford': 'West Midlands',
    'cannock': 'West Midlands',
    'burton upon trent': 'West Midlands',
    'tamworth': 'West Midlands',
    'lichfield': 'West Midlands',
    'worcester': 'West Midlands',
    'redditch': 'West Midlands',
    'kidderminster': 'West Midlands',
    'bromsgrove': 'West Midlands',
    'hereford': 'West Midlands',
    'shrewsbury': 'West Midlands',
    'telford': 'West Midlands',
    'oswestry': 'West Midlands',
    
    # East Midlands
    'nottingham': 'East Midlands',
    'leicester': 'East Midlands',
    'derby': 'East Midlands',
    'lincoln': 'East Midlands',
    'northampton': 'East Midlands',
    'peterborough': 'East Midlands',
    'mansfield': 'East Midlands',
    'chesterfield': 'East Midlands',
    'burton upon trent': 'East Midlands',
    'loughborough': 'East Midlands',
    'hinckley': 'East Midlands',
    'melton mowbray': 'East Midlands',
    'market harborough': 'East Midlands',
    'coalville': 'East Midlands',
    'ashby-de-la-zouch': 'East Midlands',
    'boston': 'East Midlands',
    'grantham': 'East Midlands',
    'sleaford': 'East Midlands',
    'spalding': 'East Midlands',
    'stamford': 'East Midlands',
    'corby': 'East Midlands',
    'kettering': 'East Midlands',
    'wellingborough': 'East Midlands',
    'daventry': 'East Midlands',
    'rushden': 'East Midlands',
    
    # East of England
    'cambridge': 'East of England',
    'peterborough': 'East of England',
    'norwich': 'East of England',
    'ipswich': 'East of England',
    'colchester': 'East of England',
    'chelmsford': 'East of England',
    'southend-on-sea': 'East of England',
    'basildon': 'East of England',
    'harlow': 'East of England',
    'braintree': 'East of England',
    'maldon': 'East of England',
    'clacton-on-sea': 'East of England',
    'great yarmouth': 'East of England',
    'kings lynn': 'East of England',
    'thetford': 'East of England',
    'dereham': 'East of England',
    'cromer': 'East of England',
    'lowestoft': 'East of England',
    'bury st edmunds': 'East of England',
    'sudbury': 'East of England',
    'haverhill': 'East of England',
    'newmarket': 'East of England',
    'felixstowe': 'East of England',
    'huntingdon': 'East of England',
    'st neots': 'East of England',
    'ely': 'East of England',
    'wisbech': 'East of England',
    'march': 'East of England',
    'st ives': 'East of England',
    
    # Yorkshire and The Humber
    'leeds': 'Yorkshire and The Humber',
    'sheffield': 'Yorkshire and The Humber',
    'bradford': 'Yorkshire and The Humber',
    'hull': 'Yorkshire and The Humber',
    'york': 'Yorkshire and The Humber',
    'doncaster': 'Yorkshire and The Humber',
    'rotherham': 'Yorkshire and The Humber',
    'barnsley': 'Yorkshire and The Humber',
    'wakefield': 'Yorkshire and The Humber',
    'huddersfield': 'Yorkshire and The Humber',
    'halifax': 'Yorkshire and The Humber',
    'harrogate': 'Yorkshire and The Humber',
    'scarborough': 'Yorkshire and The Humber',
    'middlesbrough': 'Yorkshire and The Humber',
    'grimsby': 'Yorkshire and The Humber',
    'scunthorpe': 'Yorkshire and The Humber',
    'beverley': 'Yorkshire and The Humber',
    'bridlington': 'Yorkshire and The Humber',
    'selby': 'Yorkshire and The Humber',
    'ripon': 'Yorkshire and The Humber',
    'skipton': 'Yorkshire and The Humber',
    'keighley': 'Yorkshire and The Humber',
    'dewsbury': 'Yorkshire and The Humber',
    'batley': 'Yorkshire and The Humber',
    'pontefract': 'Yorkshire and The Humber',
    'castleford': 'Yorkshire and The Humber',
    
    # North West
    'manchester': 'North West',
    'liverpool': 'North West',
    'preston': 'North West',
    'blackpool': 'North West',
    'lancaster': 'North West',
    'blackburn': 'North West',
    'burnley': 'North West',
    'bolton': 'North West',
    'bury': 'North West',
    'rochdale': 'North West',
    'oldham': 'North West',
    'stockport': 'North West',
    'tameside': 'North West',
    'trafford': 'North West',
    'salford': 'North West',
    'wigan': 'North West',
    'st helens': 'North West',
    'warrington': 'North West',
    'widnes': 'North West',
    'runcorn': 'North West',
    'chester': 'North West',
    'crewe': 'North West',
    'macclesfield': 'North West',
    'congleton': 'North West',
    'nantwich': 'North West',
    'northwich': 'North West',
    'winsford': 'North West',
    'ellesmere port': 'North West',
    'birkenhead': 'North West',
    'southport': 'North West',
    'ormskirk': 'North West',
    'skelmersdale': 'North West',
    'chorley': 'North West',
    'leyland': 'North West',
    'lytham st annes': 'North West',
    'fleetwood': 'North West',
    'morecambe': 'North West',
    'kendal': 'North West',
    'barrow-in-furness': 'North West',
    'workington': 'North West',
    'whitehaven': 'North West',
    'penrith': 'North West',
    'carlisle': 'North West',
    
    # North East
    'newcastle': 'North East',
    'newcastle upon tyne': 'North East',
    'sunderland': 'North East',
    'middlesbrough': 'North East',
    'gateshead': 'North East',
    'south shields': 'North East',
    'north shields': 'North East',
    'tynemouth': 'North East',
    'wallsend': 'North East',
    'jarrow': 'North East',
    'hebburn': 'North East',
    'washington': 'North East',
    'houghton le spring': 'North East',
    'chester le street': 'North East',
    'stanley': 'North East',
    'consett': 'North East',
    'durham': 'North East',
    'darlington': 'North East',
    'hartlepool': 'North East',
    'stockton-on-tees': 'North East',
    'redcar': 'North East',
    'saltburn': 'North East',
    'guisborough': 'North East',
    'whitby': 'North East',
    'alnwick': 'North East',
    'berwick-upon-tweed': 'North East',
    'hexham': 'North East',
    'cramlington': 'North East',
    'blyth': 'North East',
    'ashington': 'North East',
    'morpeth': 'North East',
    
    # Wales
    'cardiff': 'Wales',
    'swansea': 'Wales',
    'newport': 'Wales',
    'wrexham': 'Wales',
    'barry': 'Wales',
    'caerphilly': 'Wales',
    'bridgend': 'Wales',
    'neath': 'Wales',
    'port talbot': 'Wales',
    'cwmbran': 'Wales',
    'pontypool': 'Wales',
    'llanelli': 'Wales',
    'rhondda': 'Wales',
    'merthyr tydfil': 'Wales',
    'aberdare': 'Wales',
    'pontypridd': 'Wales',
    'caernarfon': 'Wales',
    'bangor': 'Wales',
    'colwyn bay': 'Wales',
    'rhyl': 'Wales',
    'prestatyn': 'Wales',
    'flint': 'Wales',
    'mold': 'Wales',
    'holyhead': 'Wales',
    'aberystwyth': 'Wales',
    'carmarthen': 'Wales',
    'haverfordwest': 'Wales',
    'pembroke': 'Wales',
    'tenby': 'Wales',
    'brecon': 'Wales',
    'abergavenny': 'Wales',
    'monmouth': 'Wales',
    'chepstow': 'Wales',
    
    # Scotland
    'glasgow': 'Scotland',
    'edinburgh': 'Scotland',
    'aberdeen': 'Scotland',
    'dundee': 'Scotland',
    'stirling': 'Scotland',
    'perth': 'Scotland',
    'inverness': 'Scotland',
    'paisley': 'Scotland',
    'east kilbride': 'Scotland',
    'livingston': 'Scotland',
    'hamilton': 'Scotland',
    'kirkcaldy': 'Scotland',
    'dunfermline': 'Scotland',
    'ayr': 'Scotland',
    'kilmarnock': 'Scotland',
    'irvine': 'Scotland',
    'greenock': 'Scotland',
    'motherwell': 'Scotland',
    'wishaw': 'Scotland',
    'coatbridge': 'Scotland',
    'airdrie': 'Scotland',
    'falkirk': 'Scotland',
    'cumbernauld': 'Scotland',
    'rutherglen': 'Scotland',
    'dumfries': 'Scotland',
    'stranraer': 'Scotland',
    'galashiels': 'Scotland',
    'hawick': 'Scotland',
    'peebles': 'Scotland',
    'kelso': 'Scotland',
    'jedburgh': 'Scotland',
    'elgin': 'Scotland',
    'forres': 'Scotland',
    'nairn': 'Scotland',
    'fort william': 'Scotland',
    'oban': 'Scotland',
    'campbeltown': 'Scotland',
    'stornoway': 'Scotland',
    'kirkwall': 'Scotland',
    'lerwick': 'Scotland',
    
    # Northern Ireland
    'belfast': 'Northern Ireland',
    'derry': 'Northern Ireland',
    'londonderry': 'Northern Ireland',
    'lisburn': 'Northern Ireland',
    'newtownabbey': 'Northern Ireland',
    'bangor': 'Northern Ireland',
    'craigavon': 'Northern Ireland',
    'ballymena': 'Northern Ireland',
    'newry': 'Northern Ireland',
    'carrickfergus': 'Northern Ireland',
    'coleraine': 'Northern Ireland',
    'omagh': 'Northern Ireland',
    'larne': 'Northern Ireland',
    'armagh': 'Northern Ireland',
    'dungannon': 'Northern Ireland',
    'antrim': 'Northern Ireland',
    'enniskillen': 'Northern Ireland',
    'magherafelt': 'Northern Ireland',
    'ballymoney': 'Northern Ireland',
    'downpatrick': 'Northern Ireland',
    'cookstown': 'Northern Ireland',
    'strabane': 'Northern Ireland',
    'limavady': 'Northern Ireland',
    'portrush': 'Northern Ireland',
    'portstewart': 'Northern Ireland',
    'ballycastle': 'Northern Ireland',
    'cushendall': 'Northern Ireland',
    'newcastle': 'Northern Ireland',
    'warrenpoint': 'Northern Ireland',
    'banbridge': 'Northern Ireland',
    'dromore': 'Northern Ireland',
    'hillsborough': 'Northern Ireland',
    'comber': 'Northern Ireland',
    'holywood': 'Northern Ireland',
}


def standardize_region(location: str) -> str:
    """
    Map a UK city/location to its standardized ONS NUTS1 region.
    
    Args:
        location: City or location name (case-insensitive)
    
    Returns:
        Standardized ONS region name, or 'Other' if not found
        
    Examples:
        >>> standardize_region('London')
        'London'
        >>> standardize_region('manchester')
        'North West'
        >>> standardize_region('Unknown City')
        'Other'
    """
    if not location or not isinstance(location, str):
        return 'Other'
    
    # Clean and normalize the location string
    location_clean = location.strip().lower()
    
    # Return 'Other' for empty strings after stripping
    if not location_clean:
        return 'Other'
    
    # Direct lookup in the region map
    if location_clean in REGION_MAP:
        return REGION_MAP[location_clean]
    
    # Try partial matching for compound city names
    for city, region in REGION_MAP.items():
        if city in location_clean or location_clean in city:
            logger.debug(f"Partial match: '{location}' -> '{region}' (via '{city}')")
            return region
    
    # If no match found, return 'Other'
    logger.debug(f"No region match found for location: '{location}' -> 'Other'")
    return 'Other'


def get_region_statistics() -> Dict[str, int]:
    """
    Get statistics about the region mapping dictionary.
    
    Returns:
        Dictionary with region names as keys and city counts as values
    """
    region_counts = {}
    for city, region in REGION_MAP.items():
        region_counts[region] = region_counts.get(region, 0) + 1
    
    return region_counts


def validate_region_mapping() -> Dict[str, any]:
    """
    Validate the region mapping dictionary for completeness and consistency.
    
    Returns:
        Dictionary with validation results
    """
    stats = get_region_statistics()
    
    # Expected ONS NUTS1 regions
    expected_regions = {
        'London', 'South East', 'South West', 'West Midlands', 'East Midlands',
        'East of England', 'Yorkshire and The Humber', 'North West', 'North East',
        'Wales', 'Scotland', 'Northern Ireland'
    }
    
    mapped_regions = set(stats.keys())
    
    return {
        'total_cities': len(REGION_MAP),
        'total_regions': len(mapped_regions),
        'expected_regions': expected_regions,
        'mapped_regions': mapped_regions,
        'missing_regions': expected_regions - mapped_regions,
        'extra_regions': mapped_regions - expected_regions,
        'region_counts': stats,
        'is_complete': len(expected_regions - mapped_regions) == 0
    }


# Example usage and testing
if __name__ == "__main__":
    # Test the region mapping function
    test_locations = [
        'London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow',
        'Cardiff', 'Belfast', 'Bristol', 'Liverpool', 'Newcastle',
        'Unknown City', '', None
    ]
    
    print("Region Mapping Test Results:")
    print("=" * 50)
    
    for location in test_locations:
        region = standardize_region(location)
        print(f"{location:15} -> {region}")
    
    print("\nRegion Statistics:")
    print("=" * 50)
    
    validation = validate_region_mapping()
    print(f"Total cities mapped: {validation['total_cities']}")
    print(f"Total regions: {validation['total_regions']}")
    print(f"Mapping complete: {validation['is_complete']}")
    
    if validation['missing_regions']:
        print(f"Missing regions: {validation['missing_regions']}")
    
    print("\nCities per region:")
    for region, count in sorted(validation['region_counts'].items()):
        print(f"  {region:25}: {count:3d} cities")