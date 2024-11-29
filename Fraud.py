import streamlit as st
import duckdb
import pandas as pd
from scipy.stats import chi2_contingency
from scipy.stats import ttest_ind

# DuckDB 데이터베이스 파일 경로
DB_PATH = "C:/Users/hangh/OneDrive/바탕 화면/2학년_24-2/24-2_데이터베이스/Fraud/14주차 과제/DuckDB_Fraud_VSCode/insu.db"
conn = duckdb.connect(database=DB_PATH, read_only=True)

# 쿼리 실행 및 데이터프레임 반환 함수
def duckdb(query):
    result = conn.execute(query).fetchdf() 
    return st.dataframe(result)

# Streamlit 제목
st.title("DuckDB 기반 보험 데이터 분석")
st.markdown("### 📊 데이터사이언스학과 12214258 한규현")

tab1, tab2 = st.tabs(["데이터 조회", "분석"])

# 1. Insu 데이터베이스 조회
with tab1:
    st.header("< 테이블 데이터 조회 >")
    tables = ["cust", "claim", "cntt"]
    table_icons = {"cust": "👤", "claim": "📄", "cntt": "📑"}  # 테이블별 아이콘

    for table in tables:
        with st.expander(f"{table_icons[table]} 테이블: {table}", expanded=False):
            query = f"SELECT * FROM {table} LIMIT 50"  # 테이블 내용 일부만 표시
            try:
                table_data = conn.execute(query).fetchdf()

                # 검색 기능 추가
                st.write("🔍 **데이터 검색**")
                filter_col = st.selectbox(f"{table} 테이블에서 검색할 컬럼 선택", table_data.columns)
                filter_val = st.text_input(f"{table} 테이블에서 `{filter_col}`로 검색")

                # 필터링
                if filter_val:
                    filtered_data = table_data[table_data[filter_col].astype(str).str.contains(filter_val, na=False)]
                    st.dataframe(filtered_data)
                else:
                    st.dataframe(table_data)

                # 전체 데이터 보기 버튼
                if st.button(f"전체 데이터 보기 ({table})"):
                    full_query = f"SELECT * FROM {table}"
                    full_data = conn.execute(full_query).fetchdf()
                    st.dataframe(full_data)

            except Exception as e:
                st.error(f"{table} 테이블 조회 중 오류 발생: {e}")

# 2. 분석 쿼리 결과 출력
with tab2 :
    st.header("< 분석 결과 >")

    # 보험 사기자 비율 분석
    st.subheader("보험 사기자 비율 분석")
    fraud_ratio = """
    SELECT 
        COUNT(*) AS total_customers,
        SUM(CASE WHEN SIU_CUST_YN = 'Y' THEN 1 ELSE 0 END) AS fraud_customers,
        ROUND(SUM(CASE WHEN SIU_CUST_YN = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_ratio
    FROM cust
    """
    st.write("보험 사기자 비율:")
    duckdb(fraud_ratio)

    # -----------------------------------------------------------------------------------------------------------
    # 1. 결혼 여부와 보험 사기 여부의 관계
    st.subheader("1. 결혼 여부와 보험 사기의 관계")
    st.markdown("**결혼 여부와 보험 사기 여부는 관계가 있는가?**")
    wedding_fraud_table = """
    SELECT 
        WEDD_YN AS marriage_status,
        SIU_CUST_YN,
        COUNT(*) AS count
    FROM cust
    GROUP BY WEDD_YN, SIU_CUST_YN
    ORDER BY WEDD_YN, SIU_CUST_YN
    """
    duckdb(wedding_fraud_table)
    
    # SIU_CUST_YN 기준으로 데이터를 그룹화
    wedding_fraud_df = conn.execute(wedding_fraud_table).fetchdf()
    keys = list(wedding_fraud_df['SIU_CUST_YN'].unique())
    d = {key: [] for key in keys}
    for _, row in wedding_fraud_df.iterrows():
        d[row['SIU_CUST_YN']].append(row['count'])
    row_alias = ['Not Married', 'Married']
    wedding_fraud_df = pd.DataFrame(d)
    wedding_fraud_df.index = row_alias
    
    # 독립성 검정을 위해 카이제곱 검정 수행
    results = chi2_contingency(wedding_fraud_df) 
    statistic, p_value, dof, expected_freq = results
    st.markdown("**===카이제곱 검정 결과===**")
    st.text(f"Chi-Square Statistic: {statistic:.3f}")
    st.text(f"p-value: {p_value:.5e}")
    st.text(f"Degrees of Freedom: {dof}")
    st.text("\nExpected Frequencies:")
    expected_df = pd.DataFrame(expected_freq, index=wedding_fraud_df.index, columns=wedding_fraud_df.columns)
    st.dataframe(expected_df)
    alpha = 0.05
    conclusion = (
        "귀무가설을 기각합니다. (통계적으로 유의한 관계 존재)"
        if p_value < alpha
        else "귀무가설을 채택합니다. (통계적으로 유의한 관계 없음)"
    )
    st.markdown(f"\n**결론: {conclusion}**")

    # -----------------------------------------------------------------------------------------------------------
    # 2. 결혼 여부, 성별, 특정 연령대 그룹의 사기율 분석
    st.subheader("2. 결혼 여부, 성별, 특정 연령대 그룹의 사기율 분석")
    wedd_age_fraud = '''
    SELECT
        c.WEDD_YN AS marriage_status,
        CASE
            WHEN c.SEX = 1 THEN 'Male'
            WHEN c.SEX = 2 THEN 'Female'
        END AS gender,
        CASE
            WHEN c.AGE BETWEEN 20 AND 29 THEN '20-29'
            WHEN c.AGE BETWEEN 30 AND 39 THEN '30-39'
            WHEN c.AGE BETWEEN 40 AND 49 THEN '40-49'
            WHEN c.AGE BETWEEN 50 AND 59 THEN '50-59'
            ELSE '60+' 
        END AS age_group,
        COUNT(CASE WHEN c.SIU_CUST_YN = 'Y' THEN 1 END) AS fraud_count,
        COUNT(*) AS total_count,
        ROUND(COUNT(CASE WHEN c.SIU_CUST_YN = 'Y' THEN 1 END) * 100.0 / COUNT(*),2) AS fraud_rate_percentage
    FROM
        cust c
    GROUP BY
        c.WEDD_YN,
        CASE
            WHEN c.SEX = 1 THEN 'Male'
            WHEN c.SEX = 2 THEN 'Female'
        END,
        CASE
            WHEN c.AGE BETWEEN 20 AND 29 THEN '20-29'
            WHEN c.AGE BETWEEN 30 AND 39 THEN '30-39'
            WHEN c.AGE BETWEEN 40 AND 49 THEN '40-49'
            WHEN c.AGE BETWEEN 50 AND 59 THEN '50-59'
            ELSE '60+'
        END
    ORDER BY
        marriage_status,
        gender,
        age_group;
    '''
    duckdb(wedd_age_fraud)
    
    data = {
    "": ["남자", "여자"],
    "미혼": ["30대\n10.92%\n(137/1255)", "50대\n14.81%\n(61/412)"],
    "기혼": ["20대\n33.33%\n(2/6)", "50대\n13.61%\n(366/2689)"]
    }

    df = pd.DataFrame(data)

    st.markdown("**<결혼 여부, 성별, 특정 연령대 그룹에 따른 사기율이 높은 집단>**")
    st.table(df)

    # 해석
    st.markdown("""
    **해석:**
    - 20대 기혼 남성의 사기율이 가장 높지만, 표본 수가 매우 적어 신뢰하기 어려움
    - **기혼 50대 여성**의 사기율은 13.61%로, 모든 집단 중에서 표본 수가 충분한 집단 중 가장 높은 사기율
    - **기혼 50대** 전체가 다른 연령대보다 높은 사기율을 보이고 있어, 
      기혼 50대가 보험 청구를 많이 하거나 사기와 관련된 보험 청구에 자주 연루될 가능성 존재
    """)
    
    # -----------------------------------------------------------------------------------------------------------
    # 3. 사고 보험금 청구 금액과 보험 사기의 관계
    st.subheader("3. 사고 보험금 청구 금액과 보험 사기의 관계")
    st.markdown("**사고 보험금 청구 금액과 보험 사기 여부는 관계가 있는가?**")
    
    # 보험사기 청구
    fraud_claims = '''
    SELECT DMND_AMT
    FROM claim c
    JOIN cust cu
    ON c.CUST_ID = cu.CUST_ID
    WHERE cu.SIU_CUST_YN = 'Y'
    '''
    non_fraud_claims = '''
    SELECT DMND_AMT
    FROM claim c
    JOIN cust cu
    on c.CUST_ID = cu.CUST_ID
    WHERE cu.SIU_CUST_YN = 'N'
    '''
    col1, col2 = st.columns(2)
    with col1 :
        st.markdown("**< 보험사기 청구금액 >**")
        duckdb(fraud_claims)
    with col2 :
        st.markdown("**< 정상 청구금액 >**")
        duckdb(non_fraud_claims)    
    
    def interpret_ttest_results(t_stat, p_value, alpha=0.05):
        result = {
            "t-통계량" : f"{t_stat:.3f}",
            "p-value" : f"{p_value:.3f}",
            "결론" : "귀무가설을 기각합니다. 두 그룹의 평균에 통계적으로 유의한 차이가 존재합니다."
            if p_value < alpha
            else "귀무가설을 기각하지 못합니다. 두 그룹의 평균에 통계적으로 유의한 차이가 없습니다."
        }
        return result

    st.markdown("**===t-검정 결과===**")
    
    fraud_claims_df = conn.execute(fraud_claims).fetchdf()
    non_fraud_claims_df = conn.execute(non_fraud_claims).fetchdf()
    
    t_stat, p_value = ttest_ind(
        fraud_claims_df['DMND_AMT'],
        non_fraud_claims_df['DMND_AMT'],
        equal_var=False
    )

    results = interpret_ttest_results(t_stat, p_value)
    
    st.text(f"t-통계량: {results['t-통계량']}")
    st.text(f"p-value: {results['p-value']}")
    st.markdown(f"**결론: {results['결론']}**")
    
    # --------------------------------------------------------------------------------------------------------------------    
    # 4. 보험사기자의 사고 보험금 청구 금액 비교
    st.subheader("4. 보험사기자의 사고 보험금 청구 금액 비교")
    st.markdown("**정말 보험사기자의 사고 보험금 청구 금액이 높을까?**")
    claim_amount_fraud = """
    SELECT 
        c.SIU_CUST_YN AS fraud_status,
        AVG(cl.DMND_AMT) AS avg_claim_amount,
        MIN(cl.DMND_AMT) AS min_claim_amount,
        MAX(cl.DMND_AMT) AS max_claim_amount,
        COUNT(*) AS total_claims
    FROM 
        claim cl, cust c
    WHERE 
        c.CUST_ID = cl.CUST_ID
    GROUP BY 
        c.SIU_CUST_YN
    ORDER BY 
        fraud_status;
    """
    duckdb(claim_amount_fraud)
    
    st.markdown("**< 보험사기자와 일반 고객의 청구 금액 분석 >**")

    # 해석
    st.markdown("""
    **해석:**
    - 보험사기자의 평균 청구 금액이 일반 고객보다 높음
      -> **보험사기자가 일반적으로 더 큰 금액을 청구하는 경향 존재**
    - 최대 청구 금액에서도 보험사기자가 더 높음
    - 보험사기자와 일반 고객의 청구 건수 차이를 보면 일반 고객이 훨씬 많은 청구를 함
      -> **이는 보험사기자가 더 적은 건수로 더 큰 금액을 청구하는 전략을 취할 가능성 존재**
    """)
    
    # ----------------------------------------------------------------------------------------------------------
    # 5. 가설 : 보험사기자는 고의적으로 보험 상품을 자주 변경한다
    st.subheader("5. 가설 : 보험사기자는 고의적으로 보험 상품을 자주 변경한다")
    change_fraud = '''
    SELECT 
        c.CUST_ID,
        COUNT(DISTINCT t.GOOD_CLSF_CDNM) AS product_changes
    FROM cust c
    JOIN cntt t
    ON c.CUST_ID = t.CUST_ID
    WHERE c.SIU_CUST_YN = 'Y'
    GROUP BY c.CUST_ID
    HAVING product_changes > 1
    ORDER BY product_changes DESC;
    '''
    duckdb(change_fraud)
    
    st.markdown("**< 가설 검증 >**")

    # 해석
    st.markdown("""
    **해석:**
    - 대부분의 보험사기자는 상품 변경 횟수가 적음 -> 이때 변경 횟수가 3회 미만인 사람들은 고의적으로 보험 상품을 변경했다고 보기 어려움
    - 특정 고객은 비정상적으로 높은 상품 변경 횟수를 보임 -> **의도적으로 상품을 자주 변경하여 보험 사기를 시도했을 가능성 존재**
    - 보험 상품 변경 횟수가 높은 고객은 사기 패턴일 가능성 존재 -> **상품 변경이 높은 고객은 사기 탐지의 우선 조사 대상으로 삼을 필요 있음**
    """)
    
    # ------------------------------------------------------------------------------------------------------------------------------
    # 6. 전체 고객의 평균 상품 변경 횟수와 상위 10명 고객의 상품 변경 횟수
    st.subheader("6. 전체 고객의 평균 상품 변경 횟수와 상위 10명 고객의 상품 변경 횟수")
    avg_change = '''
    SELECT 
        AVG(product_changes) AS avg_product_changes_all
    FROM (
        SELECT 
            c.CUST_ID,
            COUNT(DISTINCT t.GOOD_CLSF_CDNM) AS product_changes
        FROM cust c
        JOIN cntt t
        ON c.CUST_ID = t.CUST_ID
        GROUP BY c.CUST_ID
    ) AS product_changes_all;
    '''

    avg_change_top_10 = '''
    SELECT 
        AVG(product_changes) AS avg_product_changes_top10
    FROM (
        SELECT 
            c.CUST_ID,
            COUNT(DISTINCT t.GOOD_CLSF_CDNM) AS product_changes
        FROM cust c
        JOIN cntt t
        ON c.CUST_ID = t.CUST_ID
        GROUP BY c.CUST_ID
        ORDER BY product_changes DESC
        LIMIT 10
    ) AS product_changes_top10;
    '''
    
    col1, col2 = st.columns(2)
    with col1 :
        st.markdown("**< 전체 고객의 평균 상품 변경 횟수 >**")
        duckdb(avg_change)
    with col2 :
        st.markdown("**< 상위 10명 고객의 평균 상품 변경 횟수 >**")
        duckdb(avg_change_top_10)
    
    st.markdown('''**상위 10명의 평균 상품 변경 횟수와 전체 평균 상품 변경 횟수의 차이가 나는 것으로 보아, 
                변경 횟수가 10회 이상인 고객들에 대하여 사전에 모니터링하는 시스템을 구축할 필요 있음**''')
    
    # --------------------------------------------------------------------------------------------------
    # 7. 보험 상품 유형별 보험 사기율 분석
    st.subheader("7. 보험 상품 유형별 보험 사기율 분석")
    product_pct = '''
    SELECT 
        cn.GOOD_CLSF_CDNM AS Insurance_Product, 
        SUM(CASE WHEN c.SIU_CUST_YN = 'Y' THEN 1 ELSE 0 END) AS Fraud_Count,
        SUM(CASE WHEN c.SIU_CUST_YN = 'N' THEN 1 ELSE 0 END) AS Non_Fraud_Count,
        COUNT(*) AS Total_Count,
        (SUM(CASE WHEN c.SIU_CUST_YN = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS Fraud_Rate
    FROM cntt cn
    JOIN cust c
    ON c.CUST_ID = cn.CUST_ID
    GROUP BY cn.GOOD_CLSF_CDNM
    ORDER BY Fraud_Rate DESC;
    '''
    duckdb(product_pct)
    
    st.markdown('''**Fraud_Rate가 10%보다 높은 정기, 어린이저축, 교육, 일반저축보험을 고위험 상품군으로 분류하여,
                보험 심사 시 추가적으로 확인할 장치를 만들어 둘 필요 있음**''')
   
    # ----------------------------------------------------------------------------------------------------------
    # 8. 사고 구분 유형과 보험 사기의 관계
    st.subheader("8. 사고 구분과 보험 사기의 관계")
    st.markdown("**사고 구분 유형과 보험 사기 여부는 관계가 있는가?**")
    
    accident_fraud_table = '''
    SELECT
        c.ACCI_DVSN,
        cu.SIU_CUST_YN,
        COUNT(*) AS cnt
    FROM claim c
    JOIN cust cu
    ON c.CUST_ID = cu.CUST_ID
    GROUP BY c.ACCI_DVSN, cu.SIU_CUST_YN
    ORDER BY c.ACCI_DVSN, cu.SIU_CUST_YN
    '''
    duckdb(accident_fraud_table)
    
    accident_fraud_df = conn.execute(accident_fraud_table).fetchdf()
    keys = list(accident_fraud_df['SIU_CUST_YN'].unique())
    d = {key: [] for key in keys}
    for _, row in accident_fraud_df.iterrows():
        d[row['SIU_CUST_YN']].append(row['cnt'])

    # DataFrame으로 변환
    accident_fraud_df = pd.DataFrame(d)
    accident_fraud_df.index = ['disaster', 'Traffic Accident', 'disease']
    accident_fraud_df.columns = ['Non-Fraud', 'Fraud'] 
    accident_fraud_df.index = ['Disaster', 'Traffic Accident', 'Disease'] 

    # 독립성 검정을 위해 카이제곱 검정 수행
    results = chi2_contingency(accident_fraud_df) 
    statistic, p_value, dof, expected_freq = results
    st.markdown("**===카이제곱 검정 결과===**")
    st.text(f"Chi-Square Statistic: {statistic:.3f}")
    st.text(f"p-value: {p_value:.5e}")
    st.text(f"Degrees of Freedom: {dof}")
    st.text("\nExpected Frequencies:")
    expected_df = pd.DataFrame(expected_freq, index=accident_fraud_df.index, columns=accident_fraud_df.columns)
    st.dataframe(expected_df)
    alpha = 0.05
    conclusion = (
        "귀무가설을 기각합니다. (통계적으로 유의한 관계 존재)"
        if p_value < alpha
        else "귀무가설을 채택합니다. (통계적으로 유의한 관계 없음)"
    )
    st.markdown(f"\n**결론: {conclusion}**")
    
    # 해석
    st.markdown("""
    **해석:**
    - **사고 구분 유형과 보험 사기 여부 사이에 통계적으로 유의미한 관계 존재**
    - Disaster : 예측보다 보험 사기가 적게 일어남
    - Traffic Accident : 예측보다 보험 사기가 많이 일어남
    - Disease : 예측보다 보험 사기가 많이 일어남
    - -> **이를 통해 Traffic Accident와 Disease의 경우 추가 검토가 필요함을 알 수 있음**
    """)
    
    # ----------------------------------------------------------------------------------------------------------------
    # 9. 사고 유형별 해석
    st.subheader("9. 추가 검토 : 사고 유형별 해석")
    accident_fraud_amount = '''
    SELECT
        cl.ACCI_DVSN,
        AVG(cl.DMND_AMT) AS avg_claim_amount,
        COUNT(*) AS total_claims,
        SUM(CASE WHEN c.SIU_CUST_YN = 'Y' THEN 1 ELSE 0 END) AS fraud_claims
    FROM
        claim cl
    JOIN cust c
        ON c.CUST_ID = cl.CUST_ID
    GROUP BY
        cl.ACCI_DVSN
    ORDER BY
        fraud_claims DESC;
    '''
    duckdb(accident_fraud_amount)
    
    st.markdown("""
    **해석:**
    - Disaster (재난) : 재난 사고는 발생 빈도가 비교적 높고, 평균 청구 금액은 낮으나 사기 건수가 상당히 많음
        - **이는 소규모 청구를 통한 다수의 사기 시도가 이루어졌을 가능성이 있음**
    - Traffic Accident (교통재해) : 교통 재해는 청구 건수는 적지만, 평균 청구 금액이 상대적으로 높고 사기 비율이 낮음
        - **이는 고액 청구가 비교적 정당화되기 쉬운 사고 유형임을 나타냄**
        - 또한 특정 금액 이상의 청구에 대해 추가적인 서류나 조사가 필요함
    - Disease (질병) : 질병 사고는 빈번히 발생하며, 청구 금액도 가장 크고 사기 발생 비율이 매우 높음
        - **허위 진단서, 과잉 치료비 청구, 특정 병원과의 공모 등의 사기 가능성이 큼**
    """)
    
    # ------------------------------------------------------------------------------------------------------------------
    # 10. 가설 : 보험사기자는 비정상적으로 긴 입원/통원 기간을 청구할 가능성이 있다
    st.subheader("10. 가설 : 보험사기자는 비정상적으로 긴 입원/통원 기간을 청구할 가능성이 있다")
    hosp_term_fraud = '''
    SELECT
        cl.CUST_ID,
        AVG(cl.VLID_HOSP_OTDA) AS avg_valid_days,
        MAX(cl.VLID_HOSP_OTDA) AS max_valid_days,
        COUNT(*) AS total_claims
    FROM
        claim cl
    JOIN
        cust c ON cl.CUST_ID = c.CUST_ID
    WHERE
        c.SIU_CUST_YN = 'Y' -- 보험 사기로 등록된 고객만
    GROUP BY
        cl.CUST_ID
    HAVING
        avg_valid_days > (SELECT AVG(VLID_HOSP_OTDA) FROM claim) * 2 -- 평균의 2배 이상
    ORDER BY
        avg_valid_days DESC;
    '''
    duckdb(hosp_term_fraud)
    
    st.markdown("""
    **해석:**
    - CUST_ID = 2891은 평균 유효 일수가 95일, 최대 274일로 비정상적으로 긴 입원/통원 기간을 기록함
        - 이는 허위 입원, 불필요한 의료 절차 또는 의료 기관과의 공모 가능성을 시사
    - **총 청구 건수와 유효 일수 간의 관계**
        - 청구 건수가 많으면서 평균 유효 일수가 긴 고객
            - 지속적이고 장기간의 청구를 반복하며 보험금을 최대화하려는 패턴을 보일 가능성이 큼
        - 청구 건수가 적지만 평균 유효 일수가 긴 고객
            - 적은 횟수지만 고비용 청구를 시도했을 가능성이 큼
    
    **<결론> -> 평균 유효 일수가 비정상적으로 높은 고객은 사기 의심 사례로 분류 가능,
    추가로 병원별 데이터를 결합하여 분석하면 특정 의료 기관의 허위 청구 가능성도 확인 가능** 
    """)
    
    
   
   
   
# Streamlit 종료 시 DuckDB 연결 닫기
if 'conn' in locals():
    conn.close()
