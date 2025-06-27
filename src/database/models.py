SCHEMA_STATEMENTS = [
    '''CREATE TABLE IF NOT EXISTS tenants (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tenant_id TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_audit_at TIMESTAMP,
        total_sites INTEGER DEFAULT 0,
        total_users INTEGER DEFAULT 0
    );''',
    '''CREATE TABLE IF NOT EXISTS sites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id TEXT UNIQUE NOT NULL,
        tenant_id INTEGER REFERENCES tenants(id),
        url TEXT NOT NULL,
        title TEXT,
        description TEXT,
        created_at TIMESTAMP,
        storage_used BIGINT,
        storage_quota BIGINT,
        is_hub_site BOOLEAN DEFAULT FALSE,
        hub_site_id TEXT,
        last_modified TIMESTAMP
    );''',
    '''CREATE TABLE IF NOT EXISTS libraries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        library_id TEXT UNIQUE NOT NULL,
        site_id INTEGER REFERENCES sites(id),
        name TEXT NOT NULL,
        description TEXT,
        created_at TIMESTAMP,
        item_count INTEGER DEFAULT 0,
        is_hidden BOOLEAN DEFAULT FALSE,
        enable_versioning BOOLEAN DEFAULT TRUE,
        enable_minor_versions BOOLEAN DEFAULT FALSE
    );''',
    '''CREATE TABLE IF NOT EXISTS folders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        folder_id TEXT UNIQUE NOT NULL,
        library_id INTEGER REFERENCES libraries(id),
        parent_folder_id INTEGER REFERENCES folders(id),
        name TEXT NOT NULL,
        server_relative_url TEXT NOT NULL,
        item_count INTEGER DEFAULT 0,
        has_unique_permissions BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP,
        created_by TEXT,
        modified_at TIMESTAMP,
        modified_by TEXT
    );''',
    '''CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id TEXT UNIQUE NOT NULL,
        folder_id INTEGER REFERENCES folders(id),
        library_id INTEGER REFERENCES libraries(id),
        name TEXT NOT NULL,
        server_relative_url TEXT NOT NULL,
        size_bytes BIGINT,
        content_type TEXT,
        created_at TIMESTAMP,
        created_by TEXT,
        modified_at TIMESTAMP,
        modified_by TEXT,
        version TEXT,
        is_checked_out BOOLEAN DEFAULT FALSE,
        checked_out_by TEXT,
        has_unique_permissions BOOLEAN DEFAULT FALSE
    );''',
    '''CREATE TABLE IF NOT EXISTS permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object_type TEXT NOT NULL,
        object_id TEXT NOT NULL,
        principal_type TEXT NOT NULL,
        principal_id TEXT NOT NULL,
        principal_name TEXT,
        permission_level TEXT NOT NULL,
        is_inherited BOOLEAN DEFAULT TRUE,
        granted_at TIMESTAMP,
        granted_by TEXT
    );''',
    '''CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        owner_id TEXT,
        is_site_group BOOLEAN DEFAULT FALSE,
        site_id INTEGER REFERENCES sites(id),
        member_count INTEGER DEFAULT 0,
        last_synced TIMESTAMP
    );''',
    '''CREATE TABLE IF NOT EXISTS group_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER REFERENCES groups(id),
        user_id TEXT NOT NULL,
        added_at TIMESTAMP,
        added_by TEXT
    );''',
    '''CREATE TABLE IF NOT EXISTS audit_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT UNIQUE NOT NULL,
        tenant_id INTEGER REFERENCES tenants(id),
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        status TEXT DEFAULT 'running',
        total_sites_processed INTEGER DEFAULT 0,
        total_items_processed INTEGER DEFAULT 0,
        total_errors INTEGER DEFAULT 0,
        error_details TEXT
    );''',
    '''CREATE TABLE IF NOT EXISTS audit_checkpoints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER REFERENCES audit_runs(id),
        checkpoint_type TEXT NOT NULL,
        checkpoint_data TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );''',
    '''CREATE TABLE IF NOT EXISTS cache_entries (
        cache_key TEXT PRIMARY KEY,
        cache_value TEXT NOT NULL,
        expires_at TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );'''
]

INDEX_STATEMENTS = [
    'CREATE INDEX IF NOT EXISTS idx_sites_tenant ON sites (tenant_id);',
    'CREATE INDEX IF NOT EXISTS idx_sites_hub ON sites (hub_site_id);',
    'CREATE INDEX IF NOT EXISTS idx_libraries_site ON libraries (site_id);',
    'CREATE INDEX IF NOT EXISTS idx_folders_library ON folders (library_id);',
    'CREATE INDEX IF NOT EXISTS idx_folders_parent ON folders (parent_folder_id);',
    'CREATE INDEX IF NOT EXISTS idx_folders_permissions ON folders (has_unique_permissions);',
    'CREATE INDEX IF NOT EXISTS idx_files_folder ON files (folder_id);',
    'CREATE INDEX IF NOT EXISTS idx_files_library ON files (library_id);',
    'CREATE INDEX IF NOT EXISTS idx_files_permissions ON files (has_unique_permissions);',
    'CREATE INDEX IF NOT EXISTS idx_files_size ON files (size_bytes);',
    'CREATE INDEX IF NOT EXISTS idx_files_modified ON files (modified_at);',
    'CREATE INDEX IF NOT EXISTS idx_permissions_object ON permissions (object_type, object_id);',
    'CREATE INDEX IF NOT EXISTS idx_permissions_principal ON permissions (principal_type, principal_id);',
    'CREATE INDEX IF NOT EXISTS idx_permissions_level ON permissions (permission_level);',
    'CREATE INDEX IF NOT EXISTS idx_permissions_inherited ON permissions (is_inherited);',
    'CREATE INDEX IF NOT EXISTS idx_groups_site ON groups (site_id);',
    'CREATE INDEX IF NOT EXISTS idx_group_members_group ON group_members (group_id);',
    'CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members (user_id);',
    'CREATE INDEX IF NOT EXISTS idx_audit_runs_tenant ON audit_runs (tenant_id);',
    'CREATE INDEX IF NOT EXISTS idx_audit_runs_status ON audit_runs (status);',
    'CREATE INDEX IF NOT EXISTS idx_checkpoints_run ON audit_checkpoints (run_id);',
    'CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache_entries (expires_at);'
]

VIEW_STATEMENTS = [
    '''CREATE VIEW IF NOT EXISTS vw_permission_summary AS
    SELECT
        p.object_type,
        p.object_id,
        p.principal_type,
        p.principal_id,
        p.principal_name,
        p.permission_level,
        p.is_inherited,
        CASE
            WHEN p.object_type = 'site' THEN s.title
            WHEN p.object_type = 'library' THEN l.name
            WHEN p.object_type = 'folder' THEN fo.name
            WHEN p.object_type = 'file' THEN fi.name
        END as object_name,
        CASE
            WHEN p.object_type = 'site' THEN s.url
            WHEN p.object_type = 'library' THEN l.name
            WHEN p.object_type = 'folder' THEN fo.server_relative_url
            WHEN p.object_type = 'file' THEN fi.server_relative_url
        END as object_path
    FROM permissions p
    LEFT JOIN sites s ON p.object_type = 'site' AND p.object_id = s.site_id
    LEFT JOIN libraries l ON p.object_type = 'library' AND p.object_id = l.library_id
    LEFT JOIN folders fo ON p.object_type = 'folder' AND p.object_id = fo.folder_id
    LEFT JOIN files fi ON p.object_type = 'file' AND p.object_id = fi.file_id;''',
    '''CREATE VIEW IF NOT EXISTS vw_storage_analytics AS
    SELECT
        s.title as site_title,
        s.url as site_url,
        COUNT(DISTINCT l.id) as library_count,
        COUNT(DISTINCT f.id) as file_count,
        SUM(f.size_bytes) as total_size_bytes,
        AVG(f.size_bytes) as avg_file_size,
        MAX(f.size_bytes) as max_file_size
    FROM sites s
    LEFT JOIN libraries l ON s.id = l.site_id
    LEFT JOIN files f ON l.id = f.library_id
    GROUP BY s.id;'''
]

