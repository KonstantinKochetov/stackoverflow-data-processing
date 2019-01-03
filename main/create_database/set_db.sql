DROP TABLE IF EXISTS "users";
DROP SEQUENCE IF EXISTS users_id_seq;
CREATE SEQUENCE users_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1;

CREATE TABLE "public"."users" (
    "id" integer DEFAULT nextval('users_id_seq') NOT NULL,
    "reputation" integer NULL,
    "creationDate" timestamp NULL,
    "displayName" character varying(200) NULL,
    "emailHash" integer NULL,
    "lastAccessDate" timestamp NULL,
    "websiteUrl" character varying(1000) NULL,
    "location" character varying(100) NULL,
    "age" integer NULL,
    "aboutMe" character varying(5999) NULL,
    "views" integer NULL,
    "upVotes" integer NULL,
    "downVotes" integer NULL,
    "profileImageUrl" character varying(1000) NULL,
    "accountId" integer NULL,
    "totalNumberOfPosts" integer NULL,
    "critic" integer NULL,
    "editor" integer NULL,
    "lastMonthNumberOfPosts" integer NULL
) WITH (oids = false);
