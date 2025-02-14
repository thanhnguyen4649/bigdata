# support_functions.py
import requests
from bs4 import BeautifulSoup
import re
import time
import numpy as np
import overpy
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from unidecode import unidecode

# ---------------------------
# PHẦN A: HÀM HỖ TRỢ - GEOCODING & OVERPASS
# ---------------------------

geolocator = Nominatim(user_agent="mogi_scraper")

def normalize_address(addr):
    """
    Chuẩn hoá địa chỉ để Nominatim dễ tìm hơn:
    - Xoá nội dung trong ngoặc
    - Thay TPHCM -> Ho Chi Minh City
    - Bỏ từ "Phường" và "Quận"
    - Bỏ dấu tiếng Việt (unidecode)
    - Xoá khoảng trắng thừa
    - Thêm ", Vietnam" nếu chưa có
    """
    # 1) Bỏ tất cả nội dung trong ngoặc
    addr = re.sub(r"\([^)]*\)", "", addr)

    # 2) Thay TPHCM -> Ho Chi Minh City
    addr = addr.replace("TPHCM", "Ho Chi Minh City")

    # 3) Bỏ từ "Phường" và "Quận" nếu có
    addr = addr.replace("Phường ", "").replace("Quận ", "")

    # 4) Bỏ dấu tiếng Việt
    addr = unidecode(addr)

    # 5) Bỏ khoảng trắng thừa (nhiều space thành 1)
    addr = re.sub(r"\s+", " ", addr).strip()

    # 6) Thêm ', Vietnam' nếu chưa có
    if "Vietnam" not in addr:
        addr += ", Vietnam"

    return addr


def geocode_location(location_str):
    """
    Sử dụng Nominatim để chuyển đổi địa chỉ thành tọa độ (lat, lon).
    Trả về (lat, lon) nếu thành công, ngược lại (np.nan, np.nan).
    """
    try:
        # Gọi hàm normalize_address để chuẩn hoá địa chỉ
        address_full = normalize_address(location_str)

        loc = geolocator.geocode(address_full, timeout=10)
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception as e:
        print(f"[Geocode Error] {location_str}: {e}")
    return (np.nan, np.nan)



def distance_to_center(lat, lon):
    """
    Tính khoảng cách (km) từ (lat, lon) tới trung tâm HCM (Dinh Độc Lập).
    """
    if np.isnan(lat) or np.isnan(lon):
        return np.nan
    center_hcm = (10.77653, 106.700981)
    return geodesic((lat, lon), center_hcm).km


def get_count_avgdist(lat, lon, amenity="hospital", radius=3000):
    """
    Truy vấn Overpass API để lấy số lượng và khoảng cách trung bình (km)
    của các node có tag ["amenity"="{amenity}"] trong bán kính 'radius' (m) quanh (lat, lon).
    """
    if np.isnan(lat) or np.isnan(lon):
        return (0, 0.0)
    try:
        query = f"""
          [out:json];
          node["amenity"="{amenity}"](around:{radius},{lat},{lon});
          out body;
        """
        api = overpy.Overpass()
        result = api.query(query)
        nodes = result.nodes
        if len(nodes) == 0:
            return (0, 0.0)
        dists = [geodesic((lat, lon), (n.lat, n.lon)).km for n in nodes]
        return (len(dists), np.mean(dists))
    except Exception as e:
        print(f"[Overpass Error] {amenity} around ({lat},{lon}): {e}")
        return (0, 0.0)


def query_osm_supermarket(lat, lon, radius_km=5):
    """
    Truy vấn Overpass API để tìm các node có tag ["shop"="supermarket"]
    trong bán kính radius_km (km) quanh (lat, lon).
    """
    radius_m = radius_km * 1000  # chuyển km thành m
    query = f"""
    [out:json];
    node["shop"="supermarket"](around:{radius_m},{lat},{lon});
    out body;
    """
    try:
        api = overpy.Overpass()
        result = api.query(query)
        return result.nodes
    except Exception as e:
        print(f"Error querying OSM (supermarket) at ({lat}, {lon}): {e}")
        return []


def calculate_supermarket_stats(lat, lon, radius_km=5):
    """
    Tính số lượng và khoảng cách trung bình (km) cho siêu thị dựa theo tag "shop"="supermarket".
    """
    nodes = query_osm_supermarket(lat, lon, radius_km)
    if not nodes:
        return 0, 0.0
    distances = [geodesic((lat, lon), (node.lat, node.lon)).km for node in nodes]
    return len(nodes), sum(distances) / len(distances)


# ---------------------------
# PHẦN B: HÀM CÀO DỮ LIỆU TỪ MOGI.VN
# ---------------------------

def fetch_mogi_detail(url):
    """
    Lấy thông tin 1 tin đăng từ URL của Mogi.vn.
    Trả về list: [bedroom, restroom, location, price, square, content, date, url, lat, lon]
    """
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"[ERROR] HTTP {resp.status_code} - {url}")
            return None
        soup = BeautifulSoup(resp.text, "html.parser")

        loc_elem = soup.select_one("div.address")
        location = loc_elem.text.strip() if loc_elem else "N/A"

        price_elem = soup.select_one("div.price")
        price = "N/A"
        if price_elem:
            txt = price_elem.text.strip()
            clean_ = re.sub(r"[^\d]", "", txt)
            if clean_:
                price = clean_

        content_elem = soup.select_one("div.info-content-body")
        content = content_elem.text.strip() if content_elem else "N/A"

        sq_elem = soup.select_one("div.info-attr:contains('Diện tích sử dụng') span:nth-of-type(2)")
        square = "N/A"
        if sq_elem:
            sq_text = sq_elem.text.strip().replace("m²", "").strip()
            square = sq_text

        date_elem = soup.select_one("div.info-attr:contains('Ngày đăng') span:nth-of-type(2)")
        date_ = date_elem.text.strip() if date_elem else "N/A"

        bd_elem = soup.select_one("div.info-attr:contains('Phòng ngủ') span:nth-of-type(2)")
        bedroom = bd_elem.text.strip() if bd_elem else "N/A"

        rr_elem = soup.select_one("div.info-attr:contains('Nhà tắm') span:nth-of-type(2)")
        restroom = rr_elem.text.strip() if rr_elem else "N/A"

        lat_elem = soup.select_one('meta[property="place:location:latitude"]')
        lon_elem = soup.select_one('meta[property="place:location:longitude"]')
        lat = lat_elem.get("content", "") if lat_elem else ""
        lon = lon_elem.get("content", "") if lon_elem else ""
        try:
            lat = float(lat)
            lon = float(lon)
        except:
            lat, lon = np.nan, np.nan

        return [bedroom, restroom, location, price, square, content, date_, url, lat, lon]
    except Exception as e:
        print(f"[ERROR] fetch_mogi_detail: {url} - {e}")
        return None


def scrape_all_mogi():
    base_url = "https://mogi.vn/ho-chi-minh/thue-can-ho"
    try:
        r = requests.get(base_url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        link_elems = soup.select("a.link-overlay")
        job_urls = []
        for elem in link_elems:
            href = elem.get("href", "")
            if href and not href.startswith("http"):
                href = "https://mogi.vn" + href
            if href:
                # Nếu URL không chứa chuỗi "-id" thì bỏ qua (ignore)
                if "-id" not in href:
                    print(f"Ignoring URL (not standard page): {href}")
                    continue
                job_urls.append(href)

        print(f"Found {len(job_urls)} tin ở trang 1.")
        if len(job_urls) == 0:
            return []

        results = []
        for url in job_urls:
            print(f"\nĐang xử lý tin: {url}")
            row = fetch_mogi_detail(url)
            if row:
                results.append(row)

        return results  # trả về list (có thể rỗng)

    except Exception as e:
        print(f"[ERROR] scrape_all_mogi: {e}")
        return []


# ---------------------------
# PHẦN C: XỬ LÝ DỮ LIỆU (Transform)
# ---------------------------

def parse_price(raw_price):
    """
    Chuyển đổi chuỗi giá có thể chứa "triệu" và "nghìn" thành giá đầy đủ (VND).

    Ví dụ:
      "6 triệu 200 nghìn" -> 6*1e6 + 200*1e3 = 6200000
      "6 triệu" -> 6000000
      "2.1 triệu" -> 2100000
      "22 triệu" -> 22000000
    """
    price_lower = raw_price.lower()
    trieu_value = 0.0
    nghin_value = 0.0

    # Tìm số trước từ "triệu"
    match_trieu = re.search(r'(\d+(?:[.,]\d+)?)\s*triệu', price_lower)
    if match_trieu:
        trieu_value = float(match_trieu.group(1).replace(',', '.'))

    # Tìm số trước từ "nghìn" hoặc "ngàn"
    match_nghin = re.search(r'(\d+(?:[.,]\d+)?)\s*(nghìn|ngàn)', price_lower)
    if match_nghin:
        nghin_value = float(match_nghin.group(1).replace(',', '.'))

    price_vnd = trieu_value * 1e6 + nghin_value * 1e3
    return price_vnd


def transform_one_tin(row):
    """
    Nhận vào một row (list) chứa:
      [bedroom, restroom, location, price_str, square_str, content, date, url, lat, lon]
    Và trả về dictionary với các trường số cần thiết cho mô hình.
    """
    bedroom, restroom = row[0], row[1]
    location, price_str, square_str = row[2], row[3], row[4]
    content, date_, url_ = row[5], row[6], row[7]
    lat, lon = row[8], row[9]

    raw_price = str(price_str).strip()
    price_lower = raw_price.lower()

    if "triệu" in price_lower or "nghìn" in price_lower or "ngàn" in price_lower:
        price_vnd = parse_price(raw_price)
    else:
        # Nếu không chứa từ khóa, cố gắng chuyển đổi chuỗi thành số.
        try:
            # Nếu chuỗi có dạng số khoa học (ví dụ "6e-06") hoặc số nhỏ, giả sử nó là số triệu
            num = float(raw_price)
            if num < 1000:  # Giả sử nếu nhỏ hơn 1000, đó là giá trị theo đơn vị "triệu"
                price_vnd = num * 1e6
            else:
                price_vnd = num
        except:
            price_vnd = np.nan

    # Xử lý diện tích
    try:
        # Loại bỏ các ký tự "m²" hoặc "m2", chuyển dấu phẩy thành dấu chấm, và loại bỏ khoảng trắng thừa
        square_str_clean = str(square_str).replace("m²", "").replace("m2", "").strip()
        square_str_clean = square_str_clean.replace(",", ".")
        sq = float(square_str_clean)
    except:
        sq = np.nan

    # --- Thay đổi đoạn này ---
    # Xử lý phòng ngủ (nếu là "N/A" hay parse lỗi thì mặc định = 1)
    if not bedroom or bedroom == 'N/A':
        bd = 1
    else:
        try:
            bd = float(bedroom)
        except:
            bd = 1  # Nếu parse lỗi vẫn gán = 1

    # Xử lý phòng tắm (nếu là "N/A" hay parse lỗi thì mặc định = 1)
    if not restroom or restroom == 'N/A':
        rr = 1
    else:
        try:
            rr = float(restroom)
        except:
            rr = 1
    # --- Kết thúc thay đổi ---

    # Nếu không có lat/lon thì dùng geocoding từ địa chỉ
    try:
        lat = float(lat)
        lon = float(lon)
    except:
        lat, lon = np.nan, np.nan

    if np.isnan(lat) or np.isnan(lon):
        print("Lat/Lon không có, sử dụng geocoding từ địa chỉ...")
        lat, lon = geocode_location(location)
        time.sleep(1)

    dist_center = distance_to_center(lat, lon)

    hosp_count, hosp_avg = get_count_avgdist(lat, lon, "hospital", 5000)
    school_count, school_avg = get_count_avgdist(lat, lon, "school", 5000)
    market_count, market_avg = calculate_supermarket_stats(lat, lon, radius_km=5)

    data = {
        "price (million VND)": price_vnd,
        "square (m2)": sq,
        "bedroom": bd,
        "restroom": rr,
        "distance_to_center": dist_center,
        "hospital_count": hosp_count,
        "hospital_avg_distance": hosp_avg,
        "school_count": school_count,
        "school_avg_distance": school_avg,
        "super_market_count": market_count,
        "super_market_avg_distance": market_avg,
        "location": location,
        "date": date_,
        "url": url_
    }

    return data


def convert_np_types(data_dict):
    """
    Chuyển đổi các giá trị kiểu NumPy (np.float64, np.int64, ...) sang kiểu dữ liệu Python thuần.
    """
    new_dict = {}
    for k, v in data_dict.items():
        if isinstance(v, np.generic):
            new_dict[k] = v.item()
        else:
            new_dict[k] = v
    return new_dict
