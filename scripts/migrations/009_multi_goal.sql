-- 009_multi_goal.sql
-- Add multi-goal conversion tracking to the optimiser.
-- Each observation can now carry a goal name (e.g. 'booking', 'proposal', 'won').
-- Existing data defaults to 'conversion' for full backward compatibility.

-- 1. observations: add goal column
ALTER TABLE optimiser.observations
    ADD COLUMN goal TEXT NOT NULL DEFAULT 'conversion';

-- 2. daily_stats: add goal column
ALTER TABLE optimiser.daily_stats
    ADD COLUMN goal TEXT NOT NULL DEFAULT 'conversion';

-- 3. Replace unique constraint to include goal
ALTER TABLE optimiser.daily_stats
    DROP CONSTRAINT uq_daily_stats;

ALTER TABLE optimiser.daily_stats
    ADD CONSTRAINT uq_daily_stats UNIQUE (experiment_id, variant_id, day, goal);
