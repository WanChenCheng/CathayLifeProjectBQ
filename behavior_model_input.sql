CREATE OR REPLACE TABLE `lab-martech-cxl.Demo.BehaviorModelInput` AS

WITH raw_union AS (
  -- 合併所有來源資料
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    event_label AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.EN_Official`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    page_title AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.PV_Official`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    event_label AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.EN_Online`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    page_title AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.PV_Online`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    event_label AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.EN_Member`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    page_title AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.PV_Member`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    event_name AS action, 
    screen_id AS page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.EN_MML`
  
  UNION ALL
  
  SELECT 
    platform, 
    user_pseudo_id, 
    DATE(TIMESTAMP(event_time)) AS event_date, 
    TIMESTAMP(event_time) AS event_time, 
    page_title AS action, 
    page_location,
    session_id,
    source,
    medium
  FROM `lab-martech-cxl.Processed_data_final.SV_MML`
),

base AS (
  SELECT
    user_pseudo_id,
    event_time,
    action,
    page_location,
    source,
    medium,
    platform,
    TIMESTAMP_DIFF(
      event_time,
      LAG(event_time) OVER (PARTITION BY user_pseudo_id ORDER BY event_time),
      SECOND
    ) AS staytime,
    REGEXP_CONTAINS(LOWER(CAST(action AS STRING)), r'line給我的業務員|line分享轉傳') AS is_share_action
  FROM raw_union
),

share_flagged AS (
  SELECT
    *,
    MAX(CAST(is_share_action AS INT64)) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS has_shared
  FROM base
),

with_prev_action_time AS (
  SELECT
    *,
    LAG(event_time) OVER (
      PARTITION BY user_pseudo_id, action
      ORDER BY event_time
    ) AS prev_event_time
  FROM share_flagged
),

with_time_diff AS (
  SELECT
    *,
    TIMESTAMP_DIFF(event_time, prev_event_time, SECOND) AS diff_from_prev_same_action
  FROM with_prev_action_time
),

with_revisit_count AS (
  SELECT
    *,
    SUM(CASE 
          WHEN diff_from_prev_same_action IS NULL THEN 0
          WHEN diff_from_prev_same_action > 300 THEN 1
          ELSE 0
        END) OVER (
      PARTITION BY user_pseudo_id, action
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS revisit_count
  FROM with_time_diff
),

action_grouped AS (
  SELECT
    *,
    CASE
      ----------------------------------------------------------------
      -- 完成投保
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(完成投保)')
        THEN '完成網路投保'

      ----------------------------------------------------------------
      -- 完成 O2O
      ----------------------------------------------------------------
      WHEN platform NOT IN ('OCWeb', '會員專區') AND REGEXP_CONTAINS(LOWER(page_location), r'(dofinish|reserve-finish)') 
        THEN '完成O2O'

      ----------------------------------------------------------------
      -- 線上繳費（排除系統維護）
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(線上繳費)')
           AND NOT REGEXP_CONTAINS(action, r'系統維護')
        THEN '線上繳費'

      ----------------------------------------------------------------
      -- 方案確認
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(方案確認)')
        THEN '方案確認'

      ----------------------------------------------------------------
      -- 投保資格確認
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(投保資格確認)')
        THEN '投保資格確認'

      ----------------------------------------------------------------
      -- 資料填寫與確認
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(資料填寫與確認)')
        THEN '資料填寫與確認'

      ----------------------------------------------------------------
      -- 立即投保（排除活動開跑）
      ----------------------------------------------------------------
      WHEN (REGEXP_CONTAINS(action , r'(立即投保)') AND NOT REGEXP_CONTAINS(action , r'活動開跑'))
           OR
           (REGEXP_CONTAINS(action, r'(立即預約|立即預約投保)') AND REGEXP_CONTAINS(page_location, r'(products)') ) THEN '立即投保'

      ----------------------------------------------------------------
      -- 手機驗證碼
      ----------------------------------------------------------------
      WHEN  REGEXP_CONTAINS(LOWER(page_location), r'(doconfirmphone)')
        THEN '手機驗證碼'

      ----------------------------------------------------------------
      -- 挑選預約顧問
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(subconfirm|confirm-consultants)')
        THEN '挑選預約顧問'

      ----------------------------------------------------------------

      -- 填寫預約資料
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(reservation)')
        THEN '填寫預約資料'

      -- Line 分享轉傳
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(LOWER(action), r'(line分享轉傳|line給我的業務員)')
        THEN 'Line分享轉傳'


     -- 預約顧問與商品諮詢
      WHEN REGEXP_CONTAINS(action, r'(預約專業顧問|預約業務顧問|找顧問|團險顧問|選擇諮詢方式|選擇諮詢服務|購買諮詢|我要諮詢|線上保險諮詢師|預約業務服務)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oaweb)') THEN "預約顧問與商品諮詢"


      ----------------------------------------------------------------
      -- 保存與分享試算結果
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(保存|分享)')
           AND REGEXP_CONTAINS(action, r'(試算)')
        THEN '保存與分享試算結果'

      ----------------------------------------------------------------
      -- 保存與分享自由配/訂製組合結果
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(保存結果|分享結果)')
        THEN '保存與分享自由配、訂製組合結果'

      ----------------------------------------------------------------
      -- 查看我的保險試算結果 
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action,r'試算結果') AND REGEXP_CONTAINS(LOWER(page_location), r'(my-estimation)')
        THEN '我的保險試算結果'


      ----------------------------------------------------------------
      -- 自由配相關頁面
      ----------------------------------------------------------------

      -- 自由配－保障規劃試算結果
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(simple/estimations/m|simple/estimations/f)')
        THEN '自由配－保障規劃試算結果'

      -- 自由配－配置我的資金試算結果
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(simple/investment-gfc/m|simple/investment-gfc/f)')
        THEN '自由配－配置我的基金試算結果'

      -- 自由配－投資規劃
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(simple/investment-gfc)')
        THEN '自由配－投資規劃'

      -- 自由配－保障規劃
      WHEN  REGEXP_CONTAINS(LOWER(page_location), r'(simple/choose)')
        THEN '自由配－保障規劃'

      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(simple/combo)')
      THEN '自由配－套餐'

      -- 自由配
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(simple/landing)') OR REGEXP_CONTAINS(action, r'(自由配)')
        THEN '自由配'

      ----------------------------------------------------------------
      -- 訂製保險組合子分類
      ----------------------------------------------------------------

      -- 訂製保險組合－人身規劃試算結果                                               
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(customize/report)')
        THEN '訂製保險組合－人身規劃試算結果'

      -- 訂製保險組合－投資規劃試算結果
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(recommendation/index/plan/list)')
        THEN '訂製保險組合－投資規劃試算結果'

      -- 訂製保險組合
      WHEN REGEXP_CONTAINS(action, r'(保險組合|客製化|三分鐘)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(customize|recommendation)')
        THEN '訂製保險組合'
        

      ----------------------------------------------------------------
      -- 試算子分類
      ----------------------------------------------------------------


      ----------------------------------------------------------------
      -- 試算：健康醫療險
      ----------------------------------------------------------------

      -- 試算: 官網-健康醫療險-實支實付-試算
      WHEN (
          (
            REGEXP_CONTAINS(LOWER(page_location), r'(estimation/fx8|estimation/fx9|estimation/fg1|estimation/fg2)')
          )
          OR (
            REGEXP_CONTAINS(action, r'(健康|醫療)') AND REGEXP_CONTAINS(action, r'(實支實付)') AND REGEXP_CONTAINS(action, r'(試算)')
          )
        )
        THEN '試算: 健康醫療險－實支實付－試算'

      -- 試算: 官網-健康醫療險-重大疾病-試算
      WHEN (
          (
            REGEXP_CONTAINS(LOWER(page_location), r'(estimation/cfe|estimation/cfg|estimation/dqa|estimation/vm6|estimation/zci|estimation/c51)')
          )
          OR (
            REGEXP_CONTAINS(action, r'(健康|醫療)') AND REGEXP_CONTAINS(action, r'(重大疾病|傷病|重大)') AND REGEXP_CONTAINS(action, r'(試算)')
          )
        )
        THEN '試算: 健康醫療險－重大疾病－試算'

      -- 試算: 官網-健康醫療險-住院手術-試算
      WHEN (
          (
          REGEXP_CONTAINS(LOWER(page_location), r'(estimation/cfb|estimation/cfc|estimation/l66|estimation/agg|estimation/wy2|estimation/ds4|estimation/eac|estimation/fz4|estimation/jw3|estimation/jwc|estimation/ecb)')
          )
          OR (
            REGEXP_CONTAINS(action, r'(健康|醫療)') AND REGEXP_CONTAINS(action, r'(住院|手術)') AND REGEXP_CONTAINS(action, r'(試算)')
          )
        )
        THEN '試算: 健康醫療險－住院手術－試算'

      -- 試算: 官網-健康醫療險-長期照顧-試算
      WHEN (
          (
            REGEXP_CONTAINS(LOWER(page_location), r'(estimation/vs8|estimation/my|estimation/cff|estimation/jd2)')
          )
          OR (
            REGEXP_CONTAINS(action, r'(健康醫療)') AND REGEXP_CONTAINS(action, r'(長期照顧)') AND REGEXP_CONTAINS(action, r'(試算)')
          )
        )
        THEN '試算: 健康醫療險－長期照顧－試算'



      #試算: 網投-健康醫療險-試算
      WHEN
        (REGEXP_CONTAINS(LOWER(page_location), r'(oim1_5000/prompt|oim0_5000/prompt|oif2_5000/prompt)') AND REGEXP_CONTAINS(LOWER(page_location), r'(page=calculator)'))
        OR 
        (REGEXP_CONTAINS(LOWER(action), r'(icare|ihealth|重大疾病|醫療|健康)') AND REGEXP_CONTAINS(action, r'(試算)'))
        OR
        (REGEXP_CONTAINS(LOWER(action), r'(享保障)')  AND REGEXP_CONTAINS(LOWER(page_location), r'(oif2_5000)') AND REGEXP_CONTAINS(action, r'(試算)'))
      THEN '試算: 網投－健康險－試算'



      #試算: 官網-意外傷害險-試算
      WHEN (REGEXP_CONTAINS(action, r'(意外傷害|心路|傷害)') AND REGEXP_CONTAINS(action, r'(試算)') AND REGEXP_CONTAINS(platform, r'(官網)'))
           OR 
           (REGEXP_CONTAINS(LOWER(page_location), r'(estimation/(cfd|db1|cfn|cfj))'))
        THEN '試算: 意外傷害險－試算'



      #試算: 網投-意外傷害險-試算
      WHEN
        (
          REGEXP_CONTAINS(LOWER(page_location), r'(oid6_5000/prompt|oid4_5000/prompt|oid3_5000/prompt|oif3_5000/prompt)')
          AND REGEXP_CONTAINS(LOWER(page_location), r'(page=calculator)')
        )
        OR
        (REGEXP_CONTAINS(LOWER(action), r'(icarry|心e路|心路|意外傷害|傷害)') AND REGEXP_CONTAINS(action, r'(試算)'))
        OR
        (REGEXP_CONTAINS(LOWER(action), r'(享保障)') AND REGEXP_CONTAINS(LOWER(page_location), r'(oif3_5000)') AND REGEXP_CONTAINS(action, r'(試算)'))
      THEN '試算: 網投－意外傷害險－試算'



      #試算: 官網-壽險試算
      WHEN (
          REGEXP_CONTAINS(LOWER(page_location), r'(estimation/(k84|kc1|cfa|n66|n64))')
          OR
          (
            REGEXP_CONTAINS(action, r'(試算)')
            AND REGEXP_CONTAINS(LOWER(page_location), r'(k84|kc1|cfa|n66|n64)')
          )
          OR
          (
            REGEXP_CONTAINS(action, r'(試算)')
            AND REGEXP_CONTAINS(action, r'(基富通|壽險)')
            AND platform='官網'
          )
      )
      THEN '試算: 壽險－試算'


      #試算: 網投-壽險-試算
      WHEN (
          (REGEXP_CONTAINS(LOWER(page_location), r'(oie8_5000/prompt|oif1_5000/prompt|oie7_5000/prompt|oif4_5000/prompt)')
            AND REGEXP_CONTAINS(LOWER(page_location), r'(page=calculator)'))
          OR
          (REGEXP_CONTAINS(action, r'(iLife|壽險|基富通)') AND REGEXP_CONTAINS(action, r'(試算)'))
          OR
          (REGEXP_CONTAINS(action, r'(享保障)') AND REGEXP_CONTAINS(LOWER(page_location), r'(oif1_5000)') AND REGEXP_CONTAINS(action, r'(試算)'))
          OR
          (REGEXP_CONTAINS(action, r'(享保障)') AND REGEXP_CONTAINS(LOWER(page_location), r'(oie7_5000)') AND REGEXP_CONTAINS(action, r'(試算)'))
          OR
          (REGEXP_CONTAINS(action, r'(享保障)') AND REGEXP_CONTAINS(LOWER(page_location), r'(oif4_5000)') AND REGEXP_CONTAINS(action, r'(試算)'))
      )
      THEN '試算: 網投－壽險－試算'




      #網投投資型保險試算
      WHEN (REGEXP_CONTAINS(LOWER(page_location), r'(oii0_5000/prompt)') and REGEXP_CONTAINS(LOWER(page_location), r'(page=calculator)')) 
            or
           (REGEXP_CONTAINS(LOWER(action), r'(ifund|投資型)') and REGEXP_CONTAINS(action, r'(試算)'))  THEN '試算: 網投－投資型年金險－試算'

      #試算: 官網-還本/年金險試算

      #試算: 網投-年金險-試算
      WHEN (
          (REGEXP_CONTAINS(LOWER(page_location), r'(oic1_5000/prompt|oic2_5000/prompt)') AND REGEXP_CONTAINS(LOWER(page_location), r'(page=calculator)'))
          OR
          (REGEXP_CONTAINS(LOWER(action), r'(money|年金)') AND REGEXP_CONTAINS(action, r'(試算)'))
      )
      THEN '試算: 網投－年金險－試算'



      #試算: 網投-旅平險-試算
      WHEN (
          (REGEXP_CONTAINS(LOWER(page_location), r'(oib0_5000/prompt|oib3_5000/prompt|oib1_5000/prompt)') AND REGEXP_CONTAINS(LOWER(page_location), r'(page=calculator)'))
          OR
          (REGEXP_CONTAINS(LOWER(action), r'(e悠遊|180天|一日平安)') AND REGEXP_CONTAINS(action, r'(保費試算|立即試算)') AND NOT REGEXP_CONTAINS(action, r'(網友|提供|？)'))
      )
      THEN '試算: 網投－旅平險－試算' 
          
      #生涯推薦試算頁面


      -- 生涯推薦0-18－試算
      WHEN REGEXP_CONTAINS(action, r'(生涯推薦_0)') AND REGEXP_CONTAINS(action, r'(試算)')
        THEN '試算: 生涯推薦0-18－試算'

      -- 生涯推薦19-34－試算
      WHEN REGEXP_CONTAINS(action, r'(生涯推薦_19)') AND REGEXP_CONTAINS(action, r'(試算)')
        THEN '試算: 生涯推薦19-34－試算'

      -- 生涯推薦35-44－試算
      WHEN REGEXP_CONTAINS(action, r'(生涯推薦_35)') AND REGEXP_CONTAINS(action, r'(試算)')
        THEN '試算: 生涯推薦35-44－試算'

      -- 生涯推薦45-60－試算
      WHEN REGEXP_CONTAINS(action, r'(生涯推薦_45)') AND REGEXP_CONTAINS(action, r'(試算)')
        THEN '試算: 生涯推薦45-60－試算'

      -- 生涯推薦61歲以上－試算
      WHEN REGEXP_CONTAINS(action, r'(生涯推薦_61)') AND REGEXP_CONTAINS(action, r'(試算)')
        THEN '試算: 生涯推薦61歲以上－試算'

     
      WHEN REGEXP_CONTAINS(action, r'(生涯推薦|人生推薦)') OR REGEXP_CONTAINS(page_location,r'(life-stage)') AND NOT REGEXP_CONTAINS(action, r'(退休缺口精算機)') THEN "生涯推薦－商品資訊頁"      

      ----------------------------------------------------------------
      -- 其他試算
      ----------------------------------------------------------------

      -- 其他試算
      WHEN REGEXP_CONTAINS(action , r'(試算|輕鬆算|退休缺口精算機)')  AND NOT REGEXP_CONTAINS(action , r'(網友|/?)') THEN "試算: 其他試算"

      ----------------------------------------------------------------
      -- 好康優惠
      ----------------------------------------------------------------

      -- 好康優惠
      WHEN REGEXP_CONTAINS(action , r'(好康優惠|好禮)') 
           OR REGEXP_CONTAINS(page_location, r'(ODAV)') THEN "好康優惠"

      ----------------------------------------------------------------
      -- 找服務（尋求服務與客服）
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(
              action,
              r'(服務據點|變更|理賠|繳費|加值|開通|申請|保單服務|借款|還款|保險金給付|申辦|補發|宣告利率|諮詢|業務員|預約|業務人員|客服|服務|問題與聯繫|客戶服務|聯絡我們|金融友善|查詢|遠距|房貸)'
            )
            OR REGEXP_CONTAINS(
              LOWER(page_location),
              r'(cathaylife/services|products-and-policy|claim|change|service|apply|modify|cathaylife/customer|cathaylifeins/faq|customerservice)'
            )
         THEN '找服務（尋求服務與客服）'
        

      ----------------------------------------------------------------
      -- 商品資訊頁：旅平險
      ----------------------------------------------------------------
       WHEN platform='網投' AND REGEXP_CONTAINS(action, r'(180天|留遊學|差旅|留學|遊學|e悠遊|旅行|旅遊|旅行平安|旅平｜旅遊平安｜一日平安|旅平險|出國|短期保險)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oip2_0100|oib0_5000|oib3_5000|oib1_5000)')
        THEN '商品資訊頁－網投－旅平險'

      ----------------------------------------------------------------
      -- 商品資訊頁：健康險
      ----------------------------------------------------------------
      WHEN platform='網投' AND REGEXP_CONTAINS(action, r'(住院|醫療|icare|實支實付|ihealth|重大疾病|iHealth|iCare)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oip2_0200|oim1_5000/prompt|oi0_5000/prompt|oif2_5000/prompt)')
        THEN '商品資訊頁－網投－健康險'

      ----------------------------------------------------------------
      -- 商品資訊頁：意外險
      ----------------------------------------------------------------
      WHEN platform='網投' AND REGEXP_CONTAINS(action, r'(新iCarry傷害險|心e路平安傷害|微型傷害|傷害｜iCarry|e路平安|意外)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oip2_0300|oid6_5000/prompt|oid3_5000/prompt|oif3_5000/prompt)')
        THEN '商品資訊頁－網投－意外傷害險'

      ----------------------------------------------------------------
      -- 商品資訊頁：投資型年金險
      ----------------------------------------------------------------
      WHEN platform='網投' AND REGEXP_CONTAINS(action, r'(iFund|投資型年金|變額年金|ifund)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oip2_0600|oii0_5000/prompt)')
        THEN '商品資訊頁－網投－投資型年金險'

      ----------------------------------------------------------------
      -- 商品資訊頁：壽險
      ----------------------------------------------------------------
      WHEN platform='網投' AND REGEXP_CONTAINS(action, r'(心iLife定期壽險|享保障小額終身壽險|享保障微型定期壽險|壽險|iLife|基富通)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oip2_0400|oie8_5000/prompt|oie7_5000/prompt|oif4_5000/prompt|oif1_5000/prompt)')
        THEN '商品資訊頁－網投－壽險'


 
      ----------------------------------------------------------------
      -- 商品資訊頁：年金險
      ----------------------------------------------------------------
      WHEN platform='網投' AND REGEXP_CONTAINS(action, r'(iMoney|鑫Money|年金險)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(oip2_0500|oic2_5000/prompt|oic1_5000/prompt)')
        THEN '商品資訊頁－網投－年金險'

      ----------------------------------------------------------------
      -- 商品資訊頁：健康醫療險總覽
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(健康險|醫療|健康醫療|住院|手術|醫療保障|外溢|icare|實支實付|ihealth|重大疾病|iHealth|iCare|傷病|長期照顧)') 
          AND NOT REGEXP_CONTAINS(action, r'(意外傷害|旅行|健康醫療|壽險)') 
           OR REGEXP_CONTAINS(LOWER(page_location), r'(products/health)')
        THEN '商品資訊頁－健康醫療險'

      ----------------------------------------------------------------
      -- 商品資訊頁：意外傷害險總覽
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(意外|傷害|意外保障|新iCarry傷害險|心e路平安傷害｜iCarry|e路平安)') AND NOT REGEXP_CONTAINS(action, r'(旅)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(products/accident)')
        THEN '商品資訊頁－意外傷害險'

      ----------------------------------------------------------------
      -- 商品資訊頁：投資型保險總覽
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(投資型|變額|投資保險|理財保險|變額壽險|變額萬能壽險|變額年金)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(products/investment)')
        THEN '商品資訊頁－投資型保險'

      ----------------------------------------------------------------
      -- 商品資訊頁：壽險總覽
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(壽險|定期壽險|終身壽險|人生保障|基本保障|iLife|基富通)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(products/life-caring)')
        THEN '商品資訊頁－壽險'

      ----------------------------------------------------------------
      -- 商品資訊頁：還本與年金型保險總覽
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(年金|還本|退休|儲蓄保險|領錢|定期給付|利變年金|遞延年金)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(products/savings)')
        THEN '商品資訊頁－還本與年金型保險'

      ----------------------------------------------------------------
      -- 商品資訊頁：旅行平安險總覽
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(action, r'(旅平險|旅行|出國|旅遊|短期保險|180天|留遊學|差旅|留學|遊學|e悠遊|旅行|旅遊|旅行平安|旅平｜旅遊平安｜一日平安|旅平險|出國|短期保險|平安保險)')
           OR REGEXP_CONTAINS(LOWER(page_location), r'(products/travel)')
        THEN '商品資訊頁－旅行平安險'

      ----------------------------------------------------------------
      -- 商品資訊頁：主題商品
      ----------------------------------------------------------------
      WHEN REGEXP_CONTAINS(LOWER(page_location), r'(cathaylife/corporate|cathaylife/student|public-servants|simple-insurance|working-holiday|policy-activation|ODAH|selling-tele|co-marketing|life-stage/61up)') OR REGEXP_CONTAINS(action, r'(主題商品)')
        THEN '商品資訊頁－主題商品'

      WHEN REGEXP_CONTAINS(action, r'(保單明細|保險視圖|保險資產|投資理財數位|保險明細)') 
          OR REGEXP_CONTAINS(LOWER(page_location), r'(insurance-coverage)') 
          OR platform='資產總覽' AND page_location NOT LIKE '%recommendation%'
        THEN '保險視圖、保單明細、資產總覽、保險明細'

      ----------------------------------------------------------------
      -- 其他
      ----------------------------------------------------------------
      ELSE '其他'
    END AS action_group
  FROM with_revisit_count
)

-- 最終輸出
SELECT
  user_pseudo_id,
  event_time,
  action,
  page_location,
  source,
  medium,
  platform,
  staytime,
  CAST(has_shared AS BOOL) AS has_shared,
  revisit_count,
  action_group
FROM action_grouped
ORDER BY user_pseudo_id, event_time;
