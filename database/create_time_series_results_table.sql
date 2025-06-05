-- Create time_series_results table for storing flagged time-based patterns
-- This table stores only items that are flagged as issues, with context for explanations

CREATE TABLE IF NOT EXISTS time_series_results (
    -- Primary identification
    result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tool_run_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Analysis classification
    analysis_type VARCHAR(20) NOT NULL CHECK (analysis_type IN ('daily', 'hourly')),
    pattern_type VARCHAR(30) NOT NULL CHECK (pattern_type IN ('response_time', 'repair_time', 'volume', 'incident_count')),
    
    -- Entity information (what is flagged)
    entity_type VARCHAR(20) NOT NULL CHECK (entity_type IN ('mechanic', 'overall', 'line')),
    entity_id VARCHAR(50), -- NULL for 'overall', mechanic name/id for 'mechanic', line name for 'line'
    
    -- Time dimension (when the issue occurs)
    time_dimension VARCHAR(20) NOT NULL, -- 'day_of_week', 'hour', 'shift'
    time_value VARCHAR(20) NOT NULL, -- 'Wednesday', '10', 'morning_shift', etc.
    
    -- Issue details
    status VARCHAR(20) DEFAULT 'flagged' CHECK (status IN ('flagged', 'resolved', 'monitoring')),
    severity VARCHAR(10) DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high')),
    description TEXT NOT NULL, -- Human-readable description
    recommended_action TEXT,
    
    -- Context data for explanations (JSON format)
    context_data JSONB NOT NULL DEFAULT '{}',
    
    -- Period information
    analysis_period_start DATE,
    analysis_period_end DATE,
    
    -- Optional notes
    notes TEXT,
    
    -- Indexing for common queries
    CONSTRAINT unique_pattern_per_run UNIQUE (tool_run_id, entity_type, entity_id, pattern_type, time_dimension, time_value)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_time_series_results_entity ON time_series_results(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_time_series_results_pattern ON time_series_results(pattern_type, time_dimension, time_value);
CREATE INDEX IF NOT EXISTS idx_time_series_results_status ON time_series_results(status, severity);
CREATE INDEX IF NOT EXISTS idx_time_series_results_tool_run ON time_series_results(tool_run_id);
CREATE INDEX IF NOT EXISTS idx_time_series_results_created_at ON time_series_results(created_at);

-- Add foreign key constraint to tool_run_logs if that table exists
-- ALTER TABLE time_series_results ADD CONSTRAINT fk_time_series_tool_run 
-- FOREIGN KEY (tool_run_id) REFERENCES tool_run_logs(run_id) ON DELETE CASCADE;

-- Add comments for documentation
COMMENT ON TABLE time_series_results IS 'Stores flagged time-based patterns from time series analysis with context for explanations';
COMMENT ON COLUMN time_series_results.analysis_type IS 'Type of time series analysis: daily or hourly';
COMMENT ON COLUMN time_series_results.pattern_type IS 'What metric was analyzed: response_time, repair_time, volume, incident_count';
COMMENT ON COLUMN time_series_results.entity_type IS 'What entity is flagged: mechanic (specific person), overall (factory-wide), line (specific production line)';
COMMENT ON COLUMN time_series_results.entity_id IS 'Identifier for the entity (NULL for overall, mechanic name for mechanic, line name for line)';
COMMENT ON COLUMN time_series_results.time_dimension IS 'Time grouping: day_of_week, hour, shift';
COMMENT ON COLUMN time_series_results.time_value IS 'Specific time value: Wednesday, 10, morning_shift, etc.';
COMMENT ON COLUMN time_series_results.context_data IS 'JSON data with comparison metrics for explanations (flagged_avg, normal_avg, team_avg, variance, etc.)';

-- Example context_data structures:
-- Mechanic pattern: {"flagged_avg": "12.5 min", "normal_avg": "8.2 min", "team_avg": "8.8 min", "variance_vs_normal": "+42%", "variance_vs_team": "+36%"}
-- Overall pattern: {"flagged_avg": "20 min", "normal_avg": "15 min", "variance": "+33%", "incident_count": 45}
-- Line pattern: {"line_flagged_avg": "25 min", "line_normal_avg": "18 min", "factory_avg": "20 min", "variance_vs_normal": "+39%"}
