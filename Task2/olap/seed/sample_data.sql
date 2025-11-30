INSERT INTO raw_crm_users (user_id, username, email, contract_number, prosthetic_model, region, created_at) VALUES
('prothetic1', 'prothetic1', 'prothetic1@example.com', 'CNT-2024-001', 'BionicPRO-X1', 'RU', '2024-01-15 10:00:00'),
('prothetic2', 'prothetic2', 'prothetic2@example.com', 'CNT-2024-002', 'BionicPRO-X2', 'EU', '2024-02-20 11:30:00'),
('prothetic3', 'prothetic3', 'prothetic3@example.com', 'CNT-2024-003', 'BionicPRO-X1', 'US', '2024-03-10 14:20:00'),
('admin1', 'admin1', 'admin1@example.com', 'CNT-2024-004', 'BionicPRO-Admin', 'RU', '2024-01-01 09:00:00'),
('user1', 'user1', 'user1@example.com', 'CNT-2024-005', 'BionicPRO-X1', 'RU', '2024-01-10 08:00:00'),
('user2', 'user2', 'user2@example.com', 'CNT-2024-006', 'BionicPRO-X2', 'EU', '2024-01-12 09:30:00');

INSERT INTO raw_telemetry (event_id, user_id, event_type, metric_name, metric_value, event_timestamp, region) VALUES
('evt-001-001', 'prothetic1', 'step_count', 'steps', 5420.0, now() - INTERVAL 2 HOUR, 'RU'),
('evt-001-002', 'prothetic1', 'battery', 'battery_level', 87.5, now() - INTERVAL 1 HOUR, 'RU'),
('evt-001-003', 'prothetic1', 'motion', 'motion_quality', 92.3, now() - INTERVAL 30 MINUTE, 'RU'),

('evt-001-004', 'prothetic1', 'step_count', 'steps', 8320.0, now() - INTERVAL 1 DAY - INTERVAL 5 HOUR, 'RU'),
('evt-001-005', 'prothetic1', 'battery', 'battery_level', 65.2, now() - INTERVAL 1 DAY - INTERVAL 3 HOUR, 'RU'),
('evt-001-006', 'prothetic1', 'motion', 'motion_quality', 88.7, now() - INTERVAL 1 DAY - INTERVAL 1 HOUR, 'RU'),

('evt-002-001', 'prothetic2', 'step_count', 'steps', 6890.0, now() - INTERVAL 3 HOUR, 'EU'),
('evt-002-002', 'prothetic2', 'battery', 'battery_level', 92.1, now() - INTERVAL 2 HOUR, 'EU'),
('evt-002-003', 'prothetic2', 'motion', 'motion_quality', 95.4, now() - INTERVAL 1 HOUR, 'EU'),

('evt-003-001', 'prothetic3', 'step_count', 'steps', 4210.0, now() - INTERVAL 4 HOUR, 'US'),
('evt-003-002', 'prothetic3', 'battery', 'battery_level', 78.3, now() - INTERVAL 2 HOUR, 'US'),
('evt-003-003', 'prothetic3', 'motion', 'motion_quality', 85.6, now() - INTERVAL 1 HOUR, 'US'),

('evt-admin-001', 'admin1', 'step_count', 'steps', 5000.0, now() - INTERVAL 2 HOUR, 'RU'),
('evt-admin-002', 'admin1', 'battery', 'battery_level', 85.5, now() - INTERVAL 1 HOUR, 'RU'),
('evt-admin-003', 'admin1', 'motion', 'motion_quality', 90.2, now() - INTERVAL 30 MINUTE, 'RU'),

('evt-user1-001', 'user1', 'step_count', 'steps', 4500.0, now() - INTERVAL 2 HOUR, 'RU'),
('evt-user1-002', 'user1', 'battery', 'battery_level', 82.0, now() - INTERVAL 1 HOUR, 'RU'),
('evt-user1-003', 'user1', 'motion', 'motion_quality', 88.5, now() - INTERVAL 30 MINUTE, 'RU'),

('evt-user2-001', 'user2', 'step_count', 'steps', 6000.0, now() - INTERVAL 2 HOUR, 'EU'),
('evt-user2-002', 'user2', 'battery', 'battery_level', 90.0, now() - INTERVAL 1 HOUR, 'EU'),
('evt-user2-003', 'user2', 'motion', 'motion_quality', 93.0, now() - INTERVAL 30 MINUTE, 'EU');

INSERT INTO mart_report_user_daily (
    user_id, 
    report_date, 
    metrics.name, 
    metrics.events_count, 
    metrics.value_sum, 
    metrics.value_avg, 
    metrics.value_min, 
    metrics.value_max,
    region,
    prosthetic_model
) VALUES
(
    'prothetic1',
    today(),
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [5420.0, 87.5, 92.3],
    [5420.0, 87.5, 92.3],
    [5420.0, 87.5, 92.3],
    [5420.0, 87.5, 92.3],
    'RU',
    'BionicPRO-X1'
),
(
    'prothetic1',
    today() - INTERVAL 1 DAY,
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [8320.0, 65.2, 88.7],
    [8320.0, 65.2, 88.7],
    [8320.0, 65.2, 88.7],
    [8320.0, 65.2, 88.7],
    'RU',
    'BionicPRO-X1'
),
(
    'prothetic2',
    today(),
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [6890.0, 92.1, 95.4],
    [6890.0, 92.1, 95.4],
    [6890.0, 92.1, 95.4],
    [6890.0, 92.1, 95.4],
    'EU',
    'BionicPRO-X2'
),
(
    'prothetic3',
    today(),
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [4210.0, 78.3, 85.6],
    [4210.0, 78.3, 85.6],
    [4210.0, 78.3, 85.6],
    [4210.0, 78.3, 85.6],
    'US',
    'BionicPRO-X1'
),
(
    'admin1',
    today(),
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [5000.0, 85.5, 90.2],
    [5000.0, 85.5, 90.2],
    [5000.0, 85.5, 90.2],
    [5000.0, 85.5, 90.2],
    'RU',
    'BionicPRO-Admin'
),
(
    'admin1',
    today() - INTERVAL 1 DAY,
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [4800.0, 83.0, 89.5],
    [4800.0, 83.0, 89.5],
    [4800.0, 83.0, 89.5],
    [4800.0, 83.0, 89.5],
    'RU',
    'BionicPRO-Admin'
),
(
    'user1',
    today(),
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [4500.0, 82.0, 88.5],
    [4500.0, 82.0, 88.5],
    [4500.0, 82.0, 88.5],
    [4500.0, 82.0, 88.5],
    'RU',
    'BionicPRO-X1'
),
(
    'user2',
    today(),
    ['steps', 'battery_level', 'motion_quality'],
    [1, 1, 1],
    [6000.0, 90.0, 93.0],
    [6000.0, 90.0, 93.0],
    [6000.0, 90.0, 93.0],
    [6000.0, 90.0, 93.0],
    'EU',
    'BionicPRO-X2'
);



