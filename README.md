# 🧞‍♂️ Genieverse - Voice-Activated Data Analytics Platform

A powerful, voice-enabled data analysis platform that converts natural language to SQL queries and generates visualizations. Built with Streamlit and powered by Google Gemini AI.

![Genieverse](static/Genieverse.png)

## ✨ Features

### 🎤 **Voice-Activated Queries**
- **Speech-to-Text**: Convert spoken questions to SQL queries
- **Natural Language Processing**: Ask questions in plain English
- **Real-time Voice Recording**: Click and speak for instant results

### 🗄️ **Intelligent Data Analysis**
- **Auto SQL Generation**: Convert natural language to optimized SQL
- **Data Visualization**: Automatic chart generation from query results
- **Schema-Aware**: Understands your database structure for better queries
- **Query Routing**: Smart routing between data analysis and visualization

### 🔐 **Secure Access**
- **User Authentication**: Login system with encrypted passwords
- **Session Management**: Secure user sessions
- **Database Protection**: Safe query execution with modification detection

### 📊 **Advanced Analytics**
- **Data Profiling**: Automatic anomaly detection and data quality checks
- **Interactive Charts**: Plotly-powered visualizations
- **Chat History**: Persistent conversation history
- **Multi-table Support**: Works with complex database schemas

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Microphone access for voice features
- Google Gemini API key
- Database connection (DuckDB or Databricks)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Aswinpookkatt/Genieverse.git
   cd Genieverse
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure your settings**
   
   Edit `config.toml` with your credentials:
   ```toml
   # Database Configuration
   duckdb_path = "data/amazon.duckdb"
   
   # Databricks (optional)
   host = "your-databricks-host"
   token = "your-databricks-token"
   http_path = "your-sql-warehouse-path"
   
   # AI Configuration
   gemini_api_key = "your-gemini-api-key"
   ```

4. **Run the application**
   ```bash
   streamlit run login.py
   ```

5. **Access the app**
   - Open your browser to `http://localhost:8501`
   - Create an account or login
   - Start querying your data!

## 📁 Project Structure

```
Genieverse/
├── 🔐 Authentication
│   ├── login.py              # Login system
│   ├── home.py               # Homepage after login
│   └── users_db.duckdb       # User database
├── 🤖 AI Agents
│   ├── agents.py             # Data Engineer & Visualization agents
│   ├── router.py             # Query routing logic
│   └── prompts.py            # AI prompts and templates
├── 🎤 Voice Features
│   ├── app.py                # Main voice-enabled application
│   └── test.py               # Voice recording test page
├── 📊 Data Analysis
│   ├── utils.py              # Database utilities
│   ├── data_scanner/         # Data profiling and quality checks
│   │   ├── data_profiler.py
│   │   └── data_quality.py
│   └── data/                 # Sample datasets
├── 🎨 UI Components
│   ├── ui.py                 # UI utilities
│   └── static/               # Images and assets
└── ⚙️ Configuration
    ├── config.toml           # Configuration file
    └── requirements.txt      # Python dependencies
```

## 🎯 Usage Examples

### Voice Queries
1. Click **"🎙️ Try Voice instead"**
2. Speak your question: *"Show me the top 10 products by sales"*
3. View generated SQL and results
4. Get automatic visualizations

### Text Queries
- *"What are the highest rated products?"*
- *"Show sales trends by month"*
- *"Find products with discounts over 50%"*
- *"Create a chart of customer ratings"*

### Data Analysis
- **Anomaly Detection**: Scan for data quality issues
- **Schema Exploration**: Understand your database structure
- **Interactive Visualizations**: Bar charts, line graphs, scatter plots
- **Query History**: Review past analyses

## 🛠️ Configuration

### Database Setup

**DuckDB (Default)**:
```python
# Uses local DuckDB file
duckdb_path = "data/amazon.duckdb"
```

**Databricks**:
```toml
host = "your-workspace-url"
token = "your-access-token"
http_path = "/sql/1.0/warehouses/your-warehouse-id"
```

### AI Configuration

Get your Google Gemini API key from [Google AI Studio](https://makersuite.google.com/app/apikey):
```toml
gemini_api_key = "your-api-key-here"
```

## 🔧 Development

### Running Tests
```bash
# Test voice recording
streamlit run test.py

# Test main application
streamlit run app.py
```

### Adding New Features
1. **New Agents**: Add to `agents.py`
2. **UI Components**: Modify `ui.py`
3. **Database Functions**: Update `utils.py`
4. **Voice Features**: Enhance `app.py`

## 🗃️ Database Support

### Supported Databases
- ✅ **DuckDB** (Default) - Local analytics database
- ✅ **Databricks** - Cloud data lakehouse platform
- 🔄 **SQLite** (Coming soon)
- 🔄 **PostgreSQL** (Coming soon)

### Sample Data
The project includes sample e-commerce data:
- `amazon.csv` - Product catalog
- `customer_dim.csv` - Customer information
- `fact_table.csv` - Sales transactions
- `item_dim.csv` - Product details

## 🚨 Security Features

- **Password Encryption**: SHA-256 hashed passwords
- **SQL Injection Protection**: Query validation and sanitization
- **Modification Detection**: Warns before data-changing operations
- **Session Management**: Secure user sessions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Streamlit** - Web application framework
- **Google Gemini** - AI language model
- **streamlit-mic-recorder** - Voice recording component
- **Plotly** - Interactive visualizations
- **DuckDB** - Analytics database engine

## 📞 Support

For support open an issue on GitHub.

---

**Built with ❤️ by the Genieverse Team**

*Transform your data conversations with the power of voice and AI* 🚀
