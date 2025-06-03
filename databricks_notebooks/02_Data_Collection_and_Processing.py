# Databricks notebook source
# MAGIC %md
# MAGIC # Real Estate Data Collection & Processing
# MAGIC 
# MAGIC This notebook contains functions for:
# MAGIC - Web scraping from mogi.vn
# MAGIC - Geocoding and geospatial analysis
# MAGIC - Data transformation and feature engineering

# COMMAND ----------

# MAGIC %run ./01_Setup_Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geocoding & Geospatial Functions

# COMMAND ----------

# Initialize geocoder
geolocator = Nominatim(user_agent="databricks_mogi_scraper")

def normalize_address(addr):
    """
    Chuẩn hoá địa chỉ để Nominatim dễ tìm hơn
    """
    # Bỏ tất cả nội dung trong ngoặc
    addr = re.sub(r"\([^)]*\)", "", addr)
    
    # Thay TPHCM -> Ho Chi Minh City
    addr = addr.replace("TPHCM", "Ho Chi Minh City")
    
    # Bỏ từ "Phường" và "Quận"
    addr = addr.replace("Phường ", "").replace("Quận ", "")
    
    # Bỏ dấu tiếng Việt
    addr = unidecode(addr)
    
    # Bỏ khoảng trắng thừa
    addr = re.sub(r"\s+", " ", addr).strip()
    
    # Thêm ', Vietnam' nếu chưa có
    if "Vietnam" not in addr:
        addr += ", Vietnam"
    
    return addr

def geocode_location(location_str):
    """
    Chuyển đổi địa chỉ thành tọa độ (lat, lon)
    """
    try:
        address_full = normalize_address(location_str)
        loc = geolocator.geocode(address_full, timeout=10)
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception as e:
        print(f"[Geocode Error] {location_str}: {e}")
    return (float('nan'), float('nan'))

def distance_to_center(lat, lon):
    """
    Tính khoảng cách tới trung tâm HCM (Dinh Độc Lập)
    """
    if pd.isna(lat) or pd.isna(lon):
        return float('nan')
    center_hcm = (10.77653, 106.700981)
    return geodesic((lat, lon), center_hcm).km

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amenity Analysis Functions

# COMMAND ----------

def get_count_avgdist(lat, lon, amenity="hospital", radius=3000):
    """
    Truy vấn Overpass API để lấy số lượng và khoảng cách trung bình
    """
    if pd.isna(lat) or pd.isna(lon):
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
        return (len(dists), sum(dists) / len(dists))
    except Exception as e:
        print(f"[Overpass Error] {amenity}: {e}")
        return (0, 0.0)

def calculate_supermarket_stats(lat, lon, radius_km=5):
    """
    Tính số lượng và khoảng cách trung bình cho siêu thị
    """
    if pd.isna(lat) or pd.isna(lon):
        return 0, 0.0
    
    radius_m = radius_km * 1000
    query = f"""
    [out:json];
    node["shop"="supermarket"](around:{radius_m},{lat},{lon});
    out body;
    """
    try:
        api = overpy.Overpass()
        result = api.query(query)
        nodes = result.nodes
        if not nodes:
            return 0, 0.0
        distances = [geodesic((lat, lon), (node.lat, node.lon)).km for node in nodes]
        return len(nodes), sum(distances) / len(distances)
    except Exception as e:
        print(f"Error querying supermarket: {e}")
        return 0, 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Web Scraping Functions (Adapted for Databricks)

# COMMAND ----------

def fetch_mogi_detail(url):
    """
    Lấy thông tin chi tiết từ URL Mogi.vn
    """
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"[ERROR] HTTP {resp.status_code} - {url}")
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Extract location
        loc_elem = soup.select_one("div.address")
        location = loc_elem.text.strip() if loc_elem else "N/A"
        
        # Extract price
        price_elem = soup.select_one("div.price")
        price = "N/A"
        if price_elem:
            txt = price_elem.text.strip()
            clean_ = re.sub(r"[^\d]", "", txt)
            if clean_:
                price = clean_
        
        # Extract square
        sq_elem = soup.select_one("div.info-attr:contains('Diện tích sử dụng') span:nth-of-type(2)")
        square = "N/A"
        if sq_elem:
            sq_text = sq_elem.text.strip().replace("m²", "").strip()
            square = sq_text
        
        # Extract date
        date_elem = soup.select_one("div.info-attr:contains('Ngày đăng') span:nth-of-type(2)")
        date_ = date_elem.text.strip() if date_elem else "N/A"
        
        # Extract bedroom
        bd_elem = soup.select_one("div.info-attr:contains('Phòng ngủ') span:nth-of-type(2)")
        bedroom = bd_elem.text.strip() if bd_elem else "N/A"
        
        # Extract restroom
        rr_elem = soup.select_one("div.info-attr:contains('Nhà tắm') span:nth-of-type(2)")
        restroom = rr_elem.text.strip() if rr_elem else "N/A"
        
        # Extract coordinates
        lat_elem = soup.select_one('meta[property="place:location:latitude"]')
        lon_elem = soup.select_one('meta[property="place:location:longitude"]')
        lat = lat_elem.get("content", "") if lat_elem else ""
        lon = lon_elem.get("content", "") if lon_elem else ""
        
        try:
            lat = float(lat)
            lon = float(lon)
        except:
            lat, lon = float('nan'), float('nan')
        
        return [bedroom, restroom, location, price, square, date_, url, lat, lon]
    
    except Exception as e:
        print(f"[ERROR] fetch_mogi_detail: {url} - {e}")
        return None

def scrape_sample_data(max_pages=1):
    """
    Scrape sample data from mogi.vn (adapted for Databricks demo)
    """
    base_url = "https://mogi.vn/ho-chi-minh/thue-can-ho"
    results = []
    
    try:
        r = requests.get(base_url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        
        link_elems = soup.select("a.link-overlay")[:5]  # Limit to 5 for demo
        job_urls = []
        
        for elem in link_elems:
            href = elem.get("href", "")
            if href and not href.startswith("http"):
                href = "https://mogi.vn" + href
            if href and "-id" in href:
                job_urls.append(href)
        
        print(f"Found {len(job_urls)} URLs for processing...")
        
        for i, url in enumerate(job_urls):
            print(f"Processing {i+1}/{len(job_urls)}: {url}")
            row = fetch_mogi_detail(url)
            if row:
                results.append(row)
            time.sleep(2)  # Be respectful to the website
        
        return results
    
    except Exception as e:
        print(f"[ERROR] scrape_sample_data: {e}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def parse_price(raw_price):
    """
    Chuyển đổi chuỗi giá thành VND
    """
    if not raw_price or raw_price == "N/A":
        return float('nan')
    
    price_lower = str(raw_price).lower()
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
    return price_vnd if price_vnd > 0 else float('nan')

def transform_property_data(raw_data_list):
    """
    Transform raw scraped data into structured format
    """
    transformed_data = []
    
    for row in raw_data_list:
        if not row or len(row) < 9:
            continue
            
        bedroom, restroom, location, price_str, square_str, date_, url, lat, lon = row
        
        # Parse price
        price_vnd = parse_price(price_str)
        
        # Parse square
        try:
            square_str_clean = str(square_str).replace("m²", "").replace("m2", "").strip()
            square_str_clean = square_str_clean.replace(",", ".")
            sq = float(square_str_clean)
        except:
            sq = float('nan')
        
        # Parse bedroom/restroom with defaults
        try:
            bd = float(bedroom) if bedroom and bedroom != 'N/A' else 1
        except:
            bd = 1
            
        try:
            rr = float(restroom) if restroom and restroom != 'N/A' else 1
        except:
            rr = 1
        
        # Handle lat/lon
        if pd.isna(lat) or pd.isna(lon):
            print(f"Geocoding for: {location}")
            lat, lon = geocode_location(location)
            time.sleep(1)  # Rate limiting
        
        # Calculate features
        dist_center = distance_to_center(lat, lon)
        hosp_count, hosp_avg = get_count_avgdist(lat, lon, "hospital", 5000)
        school_count, school_avg = get_count_avgdist(lat, lon, "school", 5000)
        market_count, market_avg = calculate_supermarket_stats(lat, lon, radius_km=5)
        
        data = {
            "price_million_vnd": price_vnd / 1e6 if not pd.isna(price_vnd) else float('nan'),
            "square_m2": sq,
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
            "url": url,
            "latitude": lat,
            "longitude": lon
        }
        
        transformed_data.append(data)
        
    return transformed_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Data Collection (Demo)

# COMMAND ----------

# Uncomment to test data collection
# print("🔍 Testing data collection...")
# sample_raw_data = scrape_sample_data(max_pages=1)
# print(f"✅ Collected {len(sample_raw_data)} raw records")

# if sample_raw_data:
#     transformed_data = transform_property_data(sample_raw_data)
#     print(f"✅ Transformed {len(transformed_data)} records")
    
#     # Convert to Spark DataFrame
#     schema = StructType([
#         StructField("price_million_vnd", DoubleType(), True),
#         StructField("square_m2", DoubleType(), True),
#         StructField("bedroom", DoubleType(), True),
#         StructField("restroom", DoubleType(), True),
#         StructField("distance_to_center", DoubleType(), True),
#         StructField("hospital_count", IntegerType(), True),
#         StructField("hospital_avg_distance", DoubleType(), True),
#         StructField("school_count", IntegerType(), True),
#         StructField("school_avg_distance", DoubleType(), True),
#         StructField("super_market_count", IntegerType(), True),
#         StructField("super_market_avg_distance", DoubleType(), True),
#         StructField("location", StringType(), True),
#         StructField("date", StringType(), True),
#         StructField("url", StringType(), True),
#         StructField("latitude", DoubleType(), True),
#         StructField("longitude", DoubleType(), True)
#     ])
    
#     # Create Spark DataFrame
#     df = spark.createDataFrame([tuple(d.values()) for d in transformed_data], schema)
#     display(df)

print("✅ Data collection and processing functions ready!")
print("📊 Ready for ML model training and deployment") 