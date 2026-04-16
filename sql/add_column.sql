ALTER TABLE public.master_data
ADD COLUMN IF NOT EXISTS "事業内容" text;

ALTER TABLE public.master_data
ADD COLUMN IF NOT EXISTS "業種" text;

ALTER TABLE public.master_data
ADD COLUMN IF NOT EXISTS "業界" text;

ALTER TABLE public.master_data
ADD COLUMN IF NOT EXISTS "許可番号" text;

ALTER TABLE public.master_data
ADD COLUMN IF NOT EXISTS "メモ" text;