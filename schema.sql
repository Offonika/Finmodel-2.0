CREATE TABLE katalog (
    org_id INTEGER,
    Организация TEXT,
    nmID INTEGER,
    imtID INTEGER,
    nmUUID TEXT,
    subjectID INTEGER,
    subjectName TEXT,
    brand TEXT,
    vendorCode TEXT,
    techSize TEXT,
    sku TEXT,
    chrtID INTEGER,
    createdAt TEXT,
    updatedAt TEXT,
    PRIMARY KEY (org_id, chrtID)
);

CREATE TABLE FinOtchet (org_id INTEGER, Организация TEXT, realizationreport_id TEXT, date_from TEXT, date_to TEXT, create_dt TEXT, currency_name TEXT, suppliercontract_code TEXT, rrd_id TEXT, gi_id TEXT, dlv_prc TEXT, fix_tariff_date_from TEXT, fix_tariff_date_to TEXT, subject_name TEXT, nm_id TEXT, brand_name TEXT, sa_name TEXT, ts_name TEXT, barcode TEXT, doc_type_name TEXT, quantity TEXT, retail_price TEXT, retail_amount TEXT, sale_percent TEXT, commission_percent TEXT, office_name TEXT, supplier_oper_name TEXT, order_dt TEXT, sale_dt TEXT, rr_dt TEXT, shk_id TEXT, retail_price_withdisc_rub TEXT, delivery_amount TEXT, return_amount TEXT, delivery_rub TEXT, gi_box_type_name TEXT, product_discount_for_report TEXT, supplier_promo TEXT, ppvz_spp_prc TEXT, ppvz_kvw_prc_base TEXT, ppvz_kvw_prc TEXT, sup_rating_prc_up TEXT, is_kgvp_v2 TEXT, ppvz_sales_commission TEXT, ppvz_for_pay TEXT, ppvz_reward TEXT, acquiring_fee TEXT, acquiring_percent TEXT, payment_processing TEXT, acquiring_bank TEXT, ppvz_vw TEXT, ppvz_vw_nds TEXT, ppvz_office_name TEXT, ppvz_office_id TEXT, ppvz_supplier_id TEXT, ppvz_supplier_name TEXT, ppvz_inn TEXT, declaration_number TEXT, bonus_type_name TEXT, sticker_id TEXT, site_country TEXT, srv_dbs TEXT, penalty TEXT, additional_payment TEXT, rebill_logistic_cost TEXT, rebill_logistic_org TEXT, storage_fee TEXT, deduction TEXT, acceptance TEXT, assembly_id TEXT, kiz TEXT, srid TEXT, report_type TEXT, is_legal_entity TEXT, trbx_id TEXT, installment_cofinancing_amount TEXT, wibes_wb_discount_percent TEXT, cashback_amount TEXT, cashback_discount TEXT, PRIMARY KEY (org_id, rrd_id));

CREATE TABLE OrdersWBFlat (
    org_id INTEGER,
    Организация TEXT,
    date TEXT, lastChangeDate TEXT, warehouseName TEXT, warehouseType TEXT, countryName TEXT, oblastOkrugName TEXT, regionName TEXT, supplierArticle TEXT, nmId TEXT, barcode TEXT, category TEXT, subject TEXT, brand TEXT, techSize TEXT, incomeID TEXT, isSupply TEXT, isRealization TEXT, totalPrice TEXT, discountPercent TEXT, spp TEXT, finishedPrice TEXT, priceWithDisc TEXT, isCancel TEXT, cancelDate TEXT, sticker TEXT, gNumber TEXT, srid TEXT,
    PRIMARY KEY (org_id, srid)
);

CREATE TABLE SalesWBFlat (
    org_id INTEGER,
    Организация TEXT,
    date TEXT, lastChangeDate TEXT, warehouseName TEXT, warehouseType TEXT, countryName TEXT, oblastOkrugName TEXT, regionName TEXT, supplierArticle TEXT, nmId TEXT, barcode TEXT, category TEXT, subject TEXT, brand TEXT, techSize TEXT, incomeID TEXT, isSupply TEXT, isRealization TEXT, totalPrice TEXT, discountPercent TEXT, spp TEXT, paymentSaleAmount TEXT, forPay TEXT, finishedPrice TEXT, priceWithDisc TEXT, saleID TEXT, sticker TEXT, gNumber TEXT, srid TEXT,
    PRIMARY KEY (org_id, srid)
);

CREATE TABLE StocksWBFlat (
    org_id INTEGER,
    Организация TEXT,
    lastChangeDate TEXT, warehouseName TEXT, supplierArticle TEXT, nmId TEXT, barcode TEXT, quantity TEXT, inWayToClient TEXT, inWayFromClient TEXT, quantityFull TEXT, category TEXT, subject TEXT, brand TEXT, techSize TEXT, Price TEXT, Discount TEXT, isSupply TEXT, isRealization TEXT, SCCode TEXT,
    PRIMARY KEY (org_id, nmId, warehouseName)
);

CREATE TABLE WBTariffsCommission (
    kgvpBooking TEXT, kgvpMarketplace TEXT, kgvpPickup TEXT, kgvpSupplier TEXT, kgvpSupplierExpress TEXT, paidStorageKgvp TEXT, parentID TEXT, parentName TEXT, subjectID TEXT, subjectName TEXT
);

CREATE TABLE WBGoodsPricesFlat (
    org_id TEXT, Организация TEXT, nmID TEXT, vendorCode TEXT, currencyIsoCode4217 TEXT, discount TEXT, clubDiscount TEXT, editableSizePrice TEXT, sizeID TEXT, techSizeName TEXT, price TEXT, discountedPrice TEXT, clubDiscountedPrice TEXT, LoadDate TEXT,
    PRIMARY KEY (org_id, nmID, sizeID, LoadDate)
);

CREATE TABLE AdvCampaignsFlat (
    org_id TEXT,
    Организация TEXT,
    campaignId TEXT,
    campaignName TEXT,
    campaignType TEXT,
    campaignStatus TEXT,
    lastChangeDate TEXT,
    LoadDate TEXT,
    PRIMARY KEY (org_id, campaignId)
);

CREATE TABLE AdvCampaignsDetailsFlat (
  org_id TEXT,
  Организация TEXT,
  advertId TEXT,
  name TEXT,
  status TEXT,
  type TEXT,
  paymentType TEXT,
  startTime TEXT,
  endTime TEXT,
  createTime TEXT,
  changeTime TEXT,
  dailyBudget TEXT,
  searchPluseState TEXT,
  param_index TEXT,
  interval_begin TEXT,
  interval_end TEXT,
  price TEXT,
  subjectId TEXT,
  subjectName TEXT,
  param_active TEXT,
  nm TEXT,
  nm_active TEXT,
  LoadDate TEXT,
  PRIMARY KEY (org_id, advertId, param_index, nm)
);

CREATE TABLE WBTariffsBox (
    DateParam TEXT,
    dtNextBox TEXT,
    dtTillMax TEXT,
    warehouseName TEXT,
    geoName TEXT,
    boxDeliveryAndStorageExpr TEXT,
    boxDeliveryBase TEXT,
    boxDeliveryCoefExpr TEXT,
    boxDeliveryLiter TEXT,
    boxDeliveryMarketplaceBase TEXT,
    boxDeliveryMarketplaceCoefExpr TEXT,
    boxDeliveryMarketplaceLiter TEXT,
    boxStorageBase TEXT,
    boxStorageCoefExpr TEXT,
    boxStorageLiter TEXT,
    LoadDate TEXT
);

CREATE TABLE PaidStorageFlat (
    org_id TEXT,
    Организация TEXT,
    date TEXT,
    giId TEXT,
    chrtId TEXT,
    logWarehouseCoef TEXT,
    officeId TEXT,
    warehouse TEXT,
    warehouseCoef TEXT,
    size TEXT,
    barcode TEXT,
    subject TEXT,
    brand TEXT,
    vendorCode TEXT,
    nmId TEXT,
    volume TEXT,
    calcType TEXT,
    warehousePrice TEXT,
    barcodesCount TEXT,
    palletPlaceCode TEXT,
    palletCount TEXT,
    originalDate TEXT,
    loyaltyDiscount TEXT,
    tariffFixDate TEXT,
    tariffLowerDate TEXT,
    DateFrom TEXT,
    DateTo TEXT,
    LoadDate TEXT,
    PRIMARY KEY (org_id, date, giId, chrtId)
);

CREATE TABLE WB_NMReportHistory (
    org_id TEXT,
    Организация TEXT,
    nmID TEXT,
    imtName TEXT,
    vendorCode TEXT,
    dt TEXT,
    openCardCount TEXT,
    addToCartCount TEXT,
    ordersCount TEXT,
    ordersSumRub TEXT,
    buyoutsCount TEXT,
    buyoutsSumRub TEXT,
    buyoutPercent TEXT,
    addToCartConversion TEXT,
    cartToOrderConversion TEXT,
    LoadDate TEXT,
    PRIMARY KEY (org_id, nmID, dt)
);

CREATE TABLE AdvCampaignsFullStats (
    org_id TEXT,
    Организация TEXT,
    advertId TEXT,
    date TEXT,              -- "день" из блока days.date (UTC+03 WB), нормализуем до YYYY-MM-DD
    appType TEXT,           -- тип приложения (если есть)
    nmId TEXT,              -- товар (если уровень nm присутствует)
    nmName TEXT,            -- имя nm (если пришло)
    views TEXT,
    clicks TEXT,
    ctr TEXT,
    cpc TEXT,
    sum TEXT,
    atbs TEXT,
    orders TEXT,
    cr TEXT,
    shks TEXT,
    sum_price TEXT,
    avg_position TEXT,      -- из boosterStats (если есть для этого дня и nm; иначе пусто)
    LoadDate TEXT,
    PRIMARY KEY (org_id, advertId, date, appType, nmId)
);

CREATE INDEX idx_AdvCampDet_org_ad ON AdvCampaignsDetailsFlat(org_id, advertId);

CREATE TABLE wb_spp (
    nmID         INTEGER PRIMARY KEY,
    priceU       INTEGER NOT NULL,
    salePriceU   INTEGER NOT NULL,
    sale_pct     INTEGER NOT NULL,
    spp          INTEGER,            -- пока редко приходит → может быть NULL
    updated_at   TEXT    NOT NULL
);

