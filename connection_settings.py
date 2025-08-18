import streamlit as st
import duckdb
import sqlite3
import psycopg2
import mysql.connector
import toml
import os
from datetime import datetime
import pandas as pd

class DatabaseConnectionManager:
    """Manages database connections and settings"""
    
    def __init__(self):
        self.config_file = "config.toml"
        self.supported_databases = {
            "DuckDB": {"icon": "🦆", "description": "Fast analytical database (Local)"},
        }
        self.load_config()
    
    def load_config(self):
        """Load existing configuration or create default"""
        try:
            if os.path.exists(self.config_file):
                self.config = toml.load(self.config_file)
            else:
                self.config = {}
        except Exception as e:
            st.error(f"Error loading config: {e}")
            self.config = {}
            
        # Initialize connection state from query params (for persistence)
        self.restore_connection_from_params()
            
    def restore_connection_from_params(self):
        """Restore connection state from query parameters"""
        params = st.query_params
        
        if params.get("db_connected") == "true":
            # Restore connection state from query params
            st.session_state.connection = {
                "active": True,
                "type": params.get("db_type", "DuckDB"),
                "details": {
                    "database": params.get("db_path", "./data/amazon.duckdb")
                },
                "last_connected": params.get("db_connected_at", ""),
                "connection_obj": None
            }
        elif "connection" not in st.session_state:
            # Initialize default connection state
            st.session_state.connection = {
                "active": False,
                "type": None,
                "details": {},
                "last_connected": None,
                "connection_obj": None
            }
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, "w") as f:
                toml.dump(self.config, f)
            return True
        except Exception as e:
            st.error(f"Error saving config: {e}")
            return False
    
    def test_connection(self, db_type, connection_params):
        """Test database connection"""
        try:
            if db_type == "DuckDB":
                db_path = connection_params.get("database", ":memory:")
                conn = duckdb.connect(db_path)
                # Test with a simple query
                conn.execute("SELECT 1").fetchone()
                conn.close()
                return True, "Connection successful!"
                
            elif db_type == "SQLite":
                db_path = connection_params.get("database")
                if not db_path:
                    return False, "Database path is required"
                conn = sqlite3.connect(db_path)
                conn.execute("SELECT 1").fetchone()
                conn.close()
                return True, "Connection successful!"
                
            elif db_type == "PostgreSQL":
                conn = psycopg2.connect(
                    host=connection_params.get("host"),
                    port=connection_params.get("port", 5432),
                    database=connection_params.get("database"),
                    user=connection_params.get("user"),
                    password=connection_params.get("password")
                )
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                conn.close()
                return True, "Connection successful!"
                
            elif db_type == "MySQL":
                conn = mysql.connector.connect(
                    host=connection_params.get("host"),
                    port=connection_params.get("port", 3306),
                    database=connection_params.get("database"),
                    user=connection_params.get("user"),
                    password=connection_params.get("password")
                )
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                conn.close()
                return True, "Connection successful!"
                
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
    
    def establish_connection(self, db_type, connection_params):
        """Establish and store active connection"""
        success, message = self.test_connection(db_type, connection_params)
        
        if success:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Update session state
            st.session_state.connection = {
                "active": True,
                "type": db_type,
                "details": connection_params.copy(),
                "last_connected": current_time,
                "connection_obj": None  # Will be created when needed
            }
            
            # Set query params for persistence (similar to login)
            st.query_params.update({
                "db_connected": "true",
                "db_type": db_type,
                "db_path": connection_params.get("database", "./data/amazon.duckdb"),
                "db_connected_at": current_time
            })
            
            # Save to config
            self.config["database"] = {
                "type": db_type,
                "connection": connection_params
            }
            self.save_config()
            
            # Clear schema cache to reload with new connection
            if hasattr(st.session_state, 'schema'):
                del st.session_state.schema
                
        return success, message
    
    def disconnect(self):
        """Disconnect from database"""
        if st.session_state.connection.get("connection_obj"):
            try:
                st.session_state.connection["connection_obj"].close()
            except:
                pass
        
        st.session_state.connection = {
            "active": False,
            "type": None,
            "details": {},
            "last_connected": None,
            "connection_obj": None
        }
        
        # Clear query params for persistence
        params_to_remove = ["db_connected", "db_type", "db_path", "db_connected_at"]
        for param in params_to_remove:
            if param in st.query_params:
                del st.query_params[param]
        
        # Clear related session state
        for key in ["schema", "data_engineer", "visualization_agent", "last_df", "last_sql_query"]:
            if key in st.session_state:
                del st.session_state[key]

def render_connection_form(db_type, manager):
    """Render connection form for specific database type"""
    st.subheader(f"Connect to {db_type}")
    
    with st.form(f"{db_type.lower()}_form"):
        if db_type == "DuckDB":
            database = st.text_input(
                "Database Path", 
                value="./data/amazon.duckdb",
                help="Use ':memory:' for in-memory database or provide file path"
            )
            connection_params = {"database": database}
            
        elif db_type == "SQLite":
            database = st.text_input(
                "Database File Path", 
                placeholder="path/to/database.db",
                help="Path to SQLite database file"
            )
            connection_params = {"database": database}
            
        elif db_type in ["PostgreSQL", "MySQL"]:
            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input("Host", value="localhost")
                database = st.text_input("Database Name")
                user = st.text_input("Username")
            with col2:
                port = st.number_input(
                    "Port", 
                    value=5432 if db_type == "PostgreSQL" else 3306,
                    min_value=1,
                    max_value=65535
                )
                password = st.text_input("Password", type="password")
            
            connection_params = {
                "host": host,
                "port": port,
                "database": database,
                "user": user,
                "password": password
            }
        
        col1, col2 = st.columns(2)
        with col1:
            test_btn = st.form_submit_button("🔍 Test Connection", use_container_width=True)
        with col2:
            connect_btn = st.form_submit_button("🔗 Connect", use_container_width=True)
        
        if test_btn:
            with st.spinner("Testing connection..."):
                success, message = manager.test_connection(db_type, connection_params)
                if success:
                    st.success(message)
                else:
                    st.error(message)
        
        if connect_btn:
            with st.spinner("Establishing connection..."):
                success, message = manager.establish_connection(db_type, connection_params)
                if success:
                    st.success(f"Successfully connected to {db_type}!")
                    st.rerun()
                else:
                    st.error(message)

def show_connection_status(manager):
    """Display current connection status"""
    connection = st.session_state.connection
    
    if connection["active"]:
        # Active connection
        st.success("🟢 Database Connected")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            st.write(f"**Database Type:** {connection['type']}")
            if connection['type'] in ['PostgreSQL', 'MySQL']:
                details = connection['details']
                st.write(f"**Host:** {details.get('host')}:{details.get('port')}")
                st.write(f"**Database:** {details.get('database')}")
                st.write(f"**User:** {details.get('user')}")
            elif connection['type'] in ['DuckDB', 'SQLite']:
                st.write(f"**Database:** {connection['details'].get('database')}")
        
        with col2:
            st.write(f"**Connected Since:** {connection['last_connected']}")
            
            # Show schema info if available
            if hasattr(st.session_state, 'schema') and st.session_state.schema:
                table_count = len(st.session_state.schema)
                st.write(f"**Tables Available:** {table_count}")
        
        with col3:
            if st.button("🔌 Disconnect", type="secondary", use_container_width=True):
                manager.disconnect()
                st.success("Disconnected successfully!")
                st.rerun()
        
    else:
        # No active connection
        st.warning("🔴 No Database Connected")
        st.info("Please connect to a database to use the chat features.")

def show_connection_settings():
    """Main function to display connection settings page"""
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
    .connection-card {
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e1e5e9;
        margin: 0.5rem 0;
        background-color: #f8f9fa;
    }
    .db-option {
        padding: 0.8rem;
        border-radius: 0.3rem;
        margin: 0.3rem 0;
        cursor: pointer;
        border: 1px solid #ddd;
    }
    .db-option:hover {
        background-color: #e9ecef;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("Database Connection Settings")
    st.markdown("Configure your database connection to start using the Data Genie chatbot.")
    
    manager = DatabaseConnectionManager()
    
    # Connection Status Section
    st.header("Connection Status")
    show_connection_status(manager)
    
    st.markdown("---")
    
    # New Connection Section - only show if not connected
    if not st.session_state.connection["active"]:
        st.header("🔗 Connect to Database")
        
        # Database type selection
        db_type = st.selectbox(
            "Choose Database Type",
            options=list(manager.supported_databases.keys()),
            format_func=lambda x: f"{manager.supported_databases[x]['icon']} {x} - {manager.supported_databases[x]['description']}"
        )
        
        # Render connection form for selected database
        if db_type:
            render_connection_form(db_type, manager)

if __name__ == "__main__":
    show_connection_settings()
