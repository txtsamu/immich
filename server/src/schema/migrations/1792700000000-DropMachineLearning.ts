import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`DROP TRIGGER IF EXISTS "asset_face_update_asset_state" ON "asset";`.execute(db);

  await sql`DROP TABLE IF EXISTS "face_search", "smart_search", "ocr_search", "asset_ocr", "asset_ocr_audit" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "person_user", "person", "person_audit", "person_group_audit", "person_group" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "asset_face", "asset_face_audit" CASCADE;`.execute(db);

  await sql`ALTER TABLE "user" DROP COLUMN IF EXISTS "clusterGroupId";`.execute(db);
  await sql`DROP TABLE IF EXISTS "cluster_group_request", "cluster_group" CASCADE;`.execute(db);

  await sql`ALTER TABLE "asset" DROP COLUMN IF EXISTS "duplicateId";`.execute(db);
  await sql`ALTER TABLE "asset_job_status" DROP COLUMN IF EXISTS "facesRecognizedAt";`.execute(db);
  await sql`ALTER TABLE "asset_job_status" DROP COLUMN IF EXISTS "duplicatesDetectedAt";`.execute(db);
  await sql`ALTER TABLE "asset_job_status" DROP COLUMN IF EXISTS "ocrAt";`.execute(db);

  await sql`DROP FUNCTION IF EXISTS "person_delete_audit";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "person_user_after_insert";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "person_delete_shares";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "person_group_delete_audit";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "asset_face_audit";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "asset_face_set_asset_state";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "asset_face_update_asset_state";`.execute(db);
  await sql`DROP FUNCTION IF EXISTS "asset_ocr_delete_audit";`.execute(db);

  await sql`DROP TYPE IF EXISTS "sourcetype";`.execute(db);
  await sql`DROP TYPE IF EXISTS "person_user_role_enum";`.execute(db);

  await sql`DELETE FROM "migration_overrides" WHERE "name" ~ '(face|person|ocr|cluster_group)';`.execute(db);

  await sql`DELETE FROM "memory" WHERE "type" = 'birthday';`.execute(db);
  await sql`DELETE FROM "system_metadata" WHERE "key" = 'facial-recognition-state';`.execute(db);
}

export async function down(): Promise<void> {
  // machine learning features were removed, this cannot be reverted
}
