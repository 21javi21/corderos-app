-- Migration script to add Hall of Hate v2 tables to existing production database
-- Run this if the database already exists and needs to be updated

-- Create Hall of Hate v2 tables if they don't exist
CREATE TABLE IF NOT EXISTS hall_of_hate_v2 (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    image_filename TEXT NOT NULL,
    frame_type TEXT DEFAULT 'default',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hall_of_hate_v2_ratings (
    id SERIAL PRIMARY KEY,
    villain_id INTEGER NOT NULL,
    user_name TEXT NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 99),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(villain_id, user_name),
    FOREIGN KEY (villain_id) REFERENCES hall_of_hate_v2(id) ON DELETE CASCADE
);

-- Grant permissions to corderos_app user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO corderos_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO corderos_app;

-- Verify tables were created
\dt hall_of_hate_v2*