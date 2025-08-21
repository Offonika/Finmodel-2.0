BEGIN;

CREATE TABLE katalog_new (
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
    snapshot_date TEXT NOT NULL DEFAULT (CURRENT_DATE),
    PRIMARY KEY (org_id, chrtID, snapshot_date)
);

INSERT INTO katalog_new (
    org_id, Организация, nmID, imtID, nmUUID,
    subjectID, subjectName, brand, vendorCode,
    techSize, sku, chrtID, createdAt, updatedAt, snapshot_date
)
SELECT
    org_id, Организация, nmID, imtID, nmUUID,
    subjectID, subjectName, brand, vendorCode,
    techSize, sku, chrtID, createdAt, updatedAt, CURRENT_DATE
FROM katalog;

DROP TABLE katalog;
ALTER TABLE katalog_new RENAME TO katalog;

COMMIT;
