-- ----------------------------
-- Create independent schema
-- ----------------------------
CREATE SCHEMA IF NOT EXISTS "distlock";

-- ----------------------------
-- Table structure for mutex
-- ----------------------------
DROP TABLE IF EXISTS "distlock"."mutex";
CREATE TABLE "distlock"."mutex" (
  "mtx_name" varchar(64) COLLATE "default" NOT NULL,
  "mtx_value" varchar(64) COLLATE "default" NOT NULL,
  "mtx_expiry" int8 NOT NULL
) WITH (OIDS=FALSE);
CREATE UNIQUE INDEX "unique_mutex_name" ON "distlock"."mutex" USING btree ("mtx_name");

-- ----------------------------
-- Table structure for rwmutex
-- ----------------------------
DROP TABLE IF EXISTS "distlock"."rwmutex";
CREATE TABLE "distlock"."rwmutex" (
  "rwmtx_name" varchar(64) COLLATE "default" NOT NULL,
  "rwmtx_hmap" jsonb NOT NULL,
  "rwmtx_expiry" int8 NOT NULL,
  "rwmtx_count" int8 NOT NULL
) WITH (OIDS=FALSE);
CREATE UNIQUE INDEX "unique_rwmutex_name" ON "distlock"."rwmutex" USING btree ("rwmtx_name");

-- ----------------------------
-- Function for mutex lock
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."lock"(VARCHAR,VARCHAR,BIGINT)
RETURNS INT AS $$
DECLARE
  expiry BIGINT;
  tstamp BIGINT;
BEGIN
	SELECT CEIL(EXTRACT(EPOCH FROM NOW()) * 1000) INTO tstamp;
	SELECT "mtx_expiry" INTO expiry FROM "distlock"."mutex" WHERE "mtx_name" = $1 FOR UPDATE NOWAIT;
  IF expiry IS NOT NULL THEN
    IF expiry > tstamp THEN
      RETURN expiry - tstamp;
    END IF;
    UPDATE "distlock"."mutex" SET "mtx_value" = $2, "mtx_expiry" = $3 + tstamp WHERE "mtx_name" = $1;
  ELSE
    INSERT INTO "distlock"."mutex" ("mtx_name", "mtx_value", "mtx_expiry") VALUES ($1, $2, $3 + tstamp);
  END IF;
  RETURN -3;
EXCEPTION
	WHEN unique_violation THEN
    RETURN $3;
	WHEN lock_not_available THEN
		SELECT "mtx_expiry" INTO expiry FROM "distlock"."mutex" WHERE "mtx_name" = $1;
		IF expiry > tstamp THEN
			RETURN expiry - tstamp;
    END IF;
		RETURN 0;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for mutex unlock
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."unlock"(VARCHAR,VARCHAR,VARCHAR)
RETURNS BOOLEAN AS $$
DECLARE
	mtxvalue VARCHAR;
BEGIN
	SELECT "mtx_value" INTO mtxvalue FROM "distlock"."mutex" WHERE "mtx_name" = $1 FOR UPDATE;
	IF mtxvalue = $2 THEN
		UPDATE "distlock"."mutex" SET "mtx_expiry" = 0 WHERE "mtx_name" = $1;
    PERFORM PG_NOTIFY($3, '1');
    RETURN TRUE;
	END IF;
	RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for mutex touch
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."touch"(VARCHAR,VARCHAR,BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
	tstamp BIGINT;
	record RECORD;
	counter SMALLINT;
BEGIN
	SELECT CEIL(EXTRACT(EPOCH FROM NOW()) * 1000) INTO tstamp;
	SELECT * INTO record FROM "distlock"."mutex" WHERE "mtx_name" = $1 FOR UPDATE;
	IF record IS NOT NULL AND record."mtx_value" = $2 AND record."mtx_expiry" > tstamp THEN
		UPDATE "distlock"."mutex" SET "mtx_expiry" = tstamp + $3 WHERE "mtx_name" = $1;
		RETURN TRUE;
	END IF;
	RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for rwmutex rlock
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."rlock"(VARCHAR,VARCHAR,BIGINT)
RETURNS INT AS $$
DECLARE
	expiry BIGINT;
  tstamp BIGINT;
	record RECORD;
	counter BIGINT;
BEGIN
	SELECT CEIL(EXTRACT(EPOCH FROM NOW()) * 1000) INTO tstamp;
	SELECT * INTO record FROM "distlock"."rwmutex" WHERE "rwmtx_name" = $1 FOR UPDATE NOWAIT;
	IF record IS NOT NULL THEN
	  IF record."rwmtx_expiry" <= tstamp THEN
	    UPDATE "distlock"."rwmutex" SET "rwmtx_expiry" = $3 + tstamp, "rwmtx_count" = 1,
			  "rwmtx_hmap" = JSONB_BUILD_OBJECT($2, 1) WHERE "rwmtx_name" = $1;
      RETURN -3;
	  END IF;
		IF record."rwmtx_count" < 0 THEN
      RETURN record."rwmtx_expiry" - tstamp;
		END IF;
		SELECT "value" INTO counter FROM JSONB_EACH(record."rwmtx_hmap") WHERE "key" = $2;
		IF counter IS NULL THEN
			counter = 0;
		END IF;
		UPDATE "distlock"."rwmutex" SET "rwmtx_expiry" = $3 + tstamp, "rwmtx_count" = "rwmtx_count" + 1,
			"rwmtx_hmap" = "rwmtx_hmap" || JSONB_BUILD_OBJECT($2, counter + 1) WHERE "rwmtx_name" = $1;
	ELSE
		INSERT INTO "distlock"."rwmutex" ("rwmtx_name", "rwmtx_hmap", "rwmtx_expiry", "rwmtx_count") VALUES ($1, JSONB_BUILD_OBJECT($2, 1), $3 + tstamp, 1);
	END IF;
  RETURN -3;
EXCEPTION
  WHEN unique_violation THEN
    RETURN 0;
	WHEN lock_not_available THEN
    RETURN 0;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for rwmutex runlock
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."runlock"(VARCHAR,VARCHAR,VARCHAR,BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
	record RECORD;
	counter INT;
BEGIN
	SELECT * INTO record FROM "distlock"."rwmutex" WHERE "rwmtx_name" = $1 FOR UPDATE;
	IF record IS NOT NULL AND (record."rwmtx_count" >= 0 OR record."rwmtx_count" + $4 >= 0) THEN
		SELECT "value" INTO counter FROM JSONB_EACH(record."rwmtx_hmap") WHERE "key" = $2;
		IF counter IS NOT NULL AND counter > 0 THEN
      IF counter = 1 THEN
        UPDATE "distlock"."rwmutex" SET "rwmtx_hmap" = "rwmtx_hmap" - $2, "rwmtx_count" = "rwmtx_count" - 1 WHERE "rwmtx_name" = $1;
        IF record."rwmtx_count" = 1 OR (record."rwmtx_count" + $4) = 1 THEN
          PERFORM PG_NOTIFY($3, '1');
        END IF;
      ELSE
        UPDATE "distlock"."rwmutex" SET "rwmtx_hmap" = "rwmtx_hmap" || JSONB_BUILD_OBJECT($2, counter - 1), "rwmtx_count" = "rwmtx_count" - 1 WHERE "rwmtx_name" = $1;
      END IF;
      RETURN TRUE;
    END IF;
	END IF;
  RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for rwmutex wlock
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."wlock"(VARCHAR,VARCHAR,BIGINT,BIGINT)
RETURNS INT AS $$
DECLARE
	expiry BIGINT;
  tstamp BIGINT;
  record RECORD;
  counter SMALLINT;
BEGIN
  SELECT CEIL(EXTRACT(EPOCH FROM NOW()) * 1000) INTO tstamp;
  SELECT * INTO record FROM "distlock"."rwmutex" WHERE "rwmtx_name" = $1 FOR UPDATE NOWAIT;
  IF record IS NULL THEN
    INSERT INTO "distlock"."rwmutex" ("rwmtx_name", "rwmtx_hmap", "rwmtx_expiry", "rwmtx_count") VALUES ($1, JSONB_BUILD_OBJECT($2, 0), $3 + tstamp, 0 - $4);
		RETURN -3;
  ELSEIF record."rwmtx_expiry" <= tstamp THEN
    UPDATE "distlock"."rwmutex" SET "rwmtx_hmap" = JSONB_BUILD_OBJECT($2, 0), "rwmtx_expiry" = $3 + tstamp, "rwmtx_count" = 0 - $4 WHERE "rwmtx_name" = $1;
    RETURN -3;
  ELSE
    IF record."rwmtx_count" >= 0 THEN
      UPDATE "distlock"."rwmutex" SET "rwmtx_count" = "rwmtx_count" - $4 WHERE "rwmtx_name" = $1;
    END IF;
    IF record."rwmtx_count" = 0 OR record."rwmtx_count" + $4 = 0 THEN
      SELECT COUNT(*) INTO counter FROM JSONB_EACH(record."rwmtx_hmap");
      IF counter = 0 THEN
        UPDATE "distlock"."rwmutex" SET "rwmtx_expiry" = $3 + tstamp, "rwmtx_hmap" = JSONB_BUILD_OBJECT($2, 0) WHERE "rwmtx_name" = $1;
				RETURN -3;
      END IF;
    END IF;
    RETURN record."rwmtx_expiry" - tstamp;
  END IF;
EXCEPTION
	WHEN unique_violation THEN
		RETURN $3;
  WHEN lock_not_available THEN
		SELECT "rwmtx_expiry" INTO expiry FROM "distlock"."rwmutex" WHERE "rwmtx_name" = $1;
    IF expiry > tstamp THEN
			RETURN expiry - tstamp;
    END IF;
		RETURN 0;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for rwmutex wunlock
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."wunlock"(VARCHAR,VARCHAR,VARCHAR,BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
	record RECORD;
	counter SMALLINT;
BEGIN
	SELECT * INTO record FROM "distlock"."rwmutex" WHERE "rwmtx_name" = $1 FOR UPDATE;
	IF record IS NOT NULL AND record."rwmtx_count" < 0 THEN
		SELECT COUNT(*) INTO counter FROM JSONB_EACH(record."rwmtx_hmap") WHERE "key" = $2;
		IF counter = 1 THEN
			UPDATE "distlock"."rwmutex" SET "rwmtx_hmap" = "rwmtx_hmap" - $2, "rwmtx_count" = "rwmtx_count" + $4 WHERE "rwmtx_name" = $1;
			PERFORM PG_NOTIFY($3, '1');
			RETURN TRUE;
		END IF;
	END IF;
	RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- ----------------------------
-- Function for rwmutex rwtouch
-- ----------------------------
CREATE OR REPLACE FUNCTION "distlock"."rwtouch"(VARCHAR,VARCHAR,BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
	tstamp BIGINT;
	record RECORD;
	counter SMALLINT;
BEGIN
	SELECT CEIL(EXTRACT(EPOCH FROM NOW()) * 1000) INTO tstamp;
	SELECT * INTO record FROM "distlock"."rwmutex" WHERE "rwmtx_name" = $1 FOR UPDATE;
	IF record IS NOT NULL AND record."rwmtx_expiry" > tstamp THEN
		SELECT COUNT(*) INTO counter FROM JSONB_EACH(record."rwmtx_hmap") WHERE "key" = $2;
		IF counter = 1 THEN
			UPDATE "distlock"."rwmutex" SET "rwmtx_expiry" = tstamp + $3 WHERE "rwmtx_name" = $1;
			RETURN TRUE;
		END IF;
	END IF;
	RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL VOLATILE;