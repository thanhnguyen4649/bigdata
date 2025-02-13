import time
import re
import numpy as np
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="test_geocode")

def normalize_address(addr):
    # 1) Bỏ tất cả nội dung trong ngoặc
    addr = re.sub(r"\([^)]*\)", "", addr)

    # 2) Thay TPHCM -> Ho Chi Minh City
    addr = addr.replace("TPHCM", "Ho Chi Minh City")

    # 3) Bỏ từ "Phường" và "Quận" nếu có
    addr = addr.replace("Phường ", "").replace("Quận ", "")

    # 4) Bỏ dấu tiếng Việt (nếu muốn chắc ăn)
    # Có thể cài unidecode: pip install unidecode
    from unidecode import unidecode
    addr = unidecode(addr)

    # 5) Bỏ khoảng trắng thừa
    addr = re.sub(r"\s+", " ", addr).strip()

    # 6) Thêm , Vietnam nếu chưa có
    if "Vietnam" not in addr:
        addr += ", Vietnam"

    return addr

def geocode_location(location_str):
    try:
        address_full = normalize_address(location_str)
        loc = geolocator.geocode(address_full, timeout=10)
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception as e:
        print(f"[Geocode Error] {location_str}: {e}")
    return (np.nan, np.nan)


# Test thử
addr_test = "Phạm Văn Đồng, Phường Linh Đông, Quận Thủ Đức (TP. Thủ Đức), TPHCM"
latlon = geocode_location(addr_test)

print("Original address:", addr_test)
print("Normalized address:", normalize_address(addr_test))
print("Lat, Lon:", latlon)
