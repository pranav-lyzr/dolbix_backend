from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import List, Optional, Union, Dict, Any
from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, Date, Boolean, 
    TIMESTAMP, func, JSON, ForeignKey, text, inspect
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import datetime
from fastapi.middleware.cors import CORSMiddleware
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
import calendar
import re
import httpx
import json
import uuid  # Add this line to import the uuid module
import os
from dotenv import load_dotenv

load_dotenv()


# Database connection with proper encoding for Japanese
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@localhost:5432/mydb")
engine = create_engine(DATABASE_URL, client_encoding='utf8')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


LYZR_API_KEY = os.getenv("LYZR_API_KEY", "sk-default-8roIgovhvCvAZtXXi4ZdosCHmnTt0LiF")
LYZR_AGENT_ID = os.getenv("LYZR_AGENT_ID", "67ccaed4f48a85278d204")
LYZR_COMPARE_AGENT_ID = os.getenv("LYZR_COMPARE_AGENT_ID")
Base = declarative_base()

# ------------------------
# Database Models (Complete)
# ------------------------


class ReportRequest(BaseModel):
    upload_ids: List[int]
    name: str  
    description: Optional[str] = None 


class MonthlyUpload(Base):
    __tablename__ = "monthly_uploads"
    
    upload_id = Column(Integer, primary_key=True, index=True)
    upload_type = Column(String, nullable=False)
    file_name = Column(String, nullable=False)
    name = Column(String, nullable=False)  # Added name field
    month = Column(String, nullable=False)  # Added name field
    year = Column(String, nullable=False)  # Added name field
    description = Column(String, nullable=True)  # Added description field
    upload_timestamp = Column(TIMESTAMP, default=func.now())

class CRMProjectRaw(Base):
    __tablename__ = "crm_projects_raw"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    upload_id = Column(Integer, ForeignKey('monthly_uploads.upload_id'))
    project_id = Column(Integer)
    status = Column(String(255))
    phase = Column(String(255))
    company_name = Column(String(255))
    department = Column(String(255))
    project_name = Column(String)
    project_manager = Column(String(255))
    pm = Column(String(255))
    order_amount_gross = Column(Numeric)
    order_amount_net = Column(Numeric)
    unit = Column(String(255))
    contract_start_date = Column(Date)
    contract_end_date = Column(Date)
    billing_method = Column(Integer)
    high_potential_mark = Column(Boolean)
    record_timestamp = Column(TIMESTAMP, default=func.now())

    class Config:
        arbitrary_types_allowed = True  # Allow arbitrary types if needed


class ERPSalesRaw(Base):
    __tablename__ = "erp_sales_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    upload_id = Column(Integer, ForeignKey('monthly_uploads.upload_id'))
    job_no = Column(String)
    client_code = Column(String(225))
    client_name = Column(String(255))
    project_name = Column(String)
    sales_amount = Column(Numeric)
    operating_profit = Column(Numeric)
    sales_date = Column(Date)
    progress_status = Column(String(225))
    record_timestamp = Column(TIMESTAMP, default=func.now())


class DataCodeRaw(Base):
    __tablename__ = "data_code_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    upload_id = Column(Integer, ForeignKey('monthly_uploads.upload_id'))
    customer_name = Column(String(255))
    department_name = Column(String(255))
    parent_code = Column(String(255))
    project_name = Column(String)
    record_timestamp = Column(TIMESTAMP, default=func.now())


class PerformanceReportGenerationHistory(Base):
    __tablename__ = "performance_report_generation_history"
    report_id = Column(Integer, primary_key=True, autoincrement=True)
    generated_timestamp = Column(TIMESTAMP, default=func.now())
    report_snapshot = Column(JSON)
    upload_id = Column(Integer, ForeignKey('monthly_uploads.upload_id'))
    name = Column(String(255))  # Add name column
    month = Column(String, nullable=False)  # Added month field
    year = Column(String, nullable=False)  # Added year field


class ReportComparisonRequest(BaseModel):
    old_report: List[Dict[str, Any]]
    new_report: List[Dict[str, Any]]
    query: str
    session_id: Optional[str] = None

class ReportComparisonResponse(BaseModel):
    comparison_id: str
    status: str
    result: Optional[str] = None
    error: Optional[str] = None

comparison_results = {}

class ReportComparison(Base):
    __tablename__ = "report_comparisons"
    comparison_id = Column(String(255), primary_key=True)
    session_id = Column(String(255))
    query_text = Column(String(1000))
    old_report_size = Column(Integer)
    new_report_size = Column(Integer)
    result = Column(String(10000))  # Adjust size as needed
    status = Column(String(50))
    error = Column(String(1000), nullable=True)
    created_at = Column(TIMESTAMP, default=func.now())
Base.metadata.create_all(bind=engine)


# ------------------------
# Pydantic Schemas with Japanese field names
# ------------------------

class CRMProjectRawModel(BaseModel):
    project_id: int = Field(..., alias="No")
    status: Optional[str] = Field(None, alias="ステータス")
    phase: Optional[str] = Field(None, alias="フェーズ")
    company_name: Optional[str] = Field(None, alias="会社名")
    department: Optional[str] = Field(None, alias="部署名")
    project_name: Optional[str] = Field(None, alias="案件名")
    project_manager: Optional[str] = Field(None, alias="PJ責任者")
    pm: Optional[str] = Field(None, alias="PM")
    order_amount_gross: Optional[float] = Field(None, alias="受注金額（グロス）")
    order_amount_net: Optional[float] = Field(None, alias="受注金額（ネット）")
    contract_start_date: Optional[str] = Field(None, alias="契約開始日")
    contract_end_date: Optional[str] = Field(None, alias="契約終了日")
    billing_method: Optional[int] = Field(None, alias="請求方法(回数)")
    unit: Optional[str] = Field(None, alias="ユニット")
    high_potential_mark: Optional[str] = Field(None, alias="見込みフラグ")

    class Config:
        from_attributes = True

class CRMUploadPayload(BaseModel):
    file_name: str
    name: str
    month: str
    year: str 
    description: Optional[str] = None
    records: List[CRMProjectRawModel]


class ERPSalesRawModel(BaseModel):
    job_no: str = Field(..., alias="JOBNo.")
    salesperson_code: Optional[str] = Field(None, alias="営業担当者コード")
    client_code: Optional[str] = Field(None, alias="クライアントコード")
    client_name: Optional[str] = Field(None, alias="クライアント名")
    project_name: Optional[str] = Field(None, alias="案件名")
    sales_posting_date: Optional[str] = Field(None, alias="売上計上日")
    progress: Optional[str] = Field(None, alias="進捗")
    sales_amount: Optional[float] = Field(None, alias="売上金額")
    operating_profit: Optional[float] = Field(None, alias="営業利益")

    @field_validator('job_no', 'salesperson_code', 'client_code', mode='before')
    @classmethod
    def convert_numbers_to_string(cls, v):
        if isinstance(v, (int, float)):
            return str(int(v))
        return v

    @field_validator('sales_amount', 'operating_profit', mode='before')
    @classmethod
    def parse_float(cls, v):
        if isinstance(v, str):
            # Handle Japanese number format (remove commas and spaces)
            v = v.strip().replace(",", "").replace(" ", "").replace("￥", "")
            if v in ["-", "—", "N/A", ""]:
                return 0.0
        try:
            return float(v)
        except ValueError:
            raise ValueError(f"Invalid number: {v}")

    model_config = ConfigDict(from_attributes=True)


class ERPSalesUploadPayload(BaseModel):
    file_name: str
    name: str
    month: str
    year: str
    description: Optional[str] = None
    records: List[ERPSalesRawModel]

class DataCodeMappingModel(BaseModel):
    customer_name: str = Field(..., alias="顧客名")
    department_name: Optional[str] = Field(None, alias="部署名")
    parent_code: Optional[str] = Field(None, alias="親コード")
    project_name: str = Field(..., alias="案件名")

    class Config:
        from_attributes = True

class DataCodeUploadPayload(BaseModel):
    file_name: str
    name: str
    month: str
    year: str
    description: Optional[str] = None
    records: List[DataCodeMappingModel]


    class Config:
        from_attributes = True
        arbitrary_types_allowed = True
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat()
        }


class ReportHistoryModel(BaseModel):
    report_id: int
    name: str  
    generated_timestamp: datetime
    report_snapshot: dict
    upload_id: int
    month: str
    year: str
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    
class ComparisonResponse(BaseModel):
    comparison_id: str
    session_id: str
    query_text: str
    status: str
    result: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
      
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    name: str = "Chat Generated Report"


# ------------------------
# Dependency & Setup
# ------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI(title="Data Upload and Reporting API")  # Japanese title
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------
# Helper Functions (Updated for Japanese)
# ------------------------

def parse_numeric(value):
    if value is None or value == "":
        return None
    if isinstance(value, str):
        # Handle Japanese number format
        value = value.strip().replace(",", "").replace("%", "").replace(" ", "").replace("￥", "")
        if value in ["-", "—", "N/A"]:
            return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0

def convert_high_potential(value: str) -> bool:
    """Convert Japanese high potential markers to boolean"""
    if not value:
        return False
    return value.strip() in ["〇", "○", "⭕", "O", "o", "◎", "◯"]

def parse_date(date_str: Optional[str]) -> Optional[datetime.date]:
    """Parse Japanese date formats with validation"""
    if not date_str or not date_str.strip():
        return None
    
    # Remove any full-width characters and normalize format
    date_str = date_str.strip().replace("年", "/").replace("月", "/").replace("日", "")
    
    # Try different date formats in priority order
    formats = [
        "%m/%d/%Y",  # MM/DD/YYYY (10/1/2024)
        "%Y/%m/%d",  # YYYY/MM/DD
        "%m/%d/%y",  # MM/DD/YY (10/1/24)
        "%y/%m/%d",  # YY/MM/DD
        "%Y年%m月%d日"  # Japanese format
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            
            # Handle 2-digit year for formats that use %y
            if fmt in ["%m/%d/%y", "%y/%m/%d"] and parsed_date.year < 2000:
                parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
            
            # Validate day of month
            year, month = parsed_date.year, parsed_date.month
            last_day = calendar.monthrange(year, month)[1]
            
            if parsed_date.day > last_day:
                return datetime(year, month, last_day).date()
            
            return parsed_date.date()
        except ValueError:
            continue
    
    # Fallback for other formats
    try:
        parts = re.split(r'[/\-\.年月日]', date_str)
        parts = [p for p in parts if p.strip()]
        
        if len(parts) == 3:
            # Try different permutations
            for fmt in ["%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime("/".join(parts), fmt).date()
                except:
                    continue
    except Exception:
        pass
    
    print(f"Could not parse date: {date_str}")
    return None

# Add Japanese month name mapping
def get_japanese_month_name(month_num):
    """Convert month number to Japanese month name"""
    jp_months = {
        1: "1月", 2: "2月", 3: "3月", 4: "4月", 
        5: "5月", 6: "6月", 7: "7月", 8: "8月",
        9: "9月", 10: "10月", 11: "11月", 12: "12月"
    }
    return jp_months.get(month_num)

def get_english_month_name(month_num):
    """Convert month number to English month name"""
    eng_months = {
        1: "January", 2: "February", 3: "March", 4: "April", 
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    return eng_months.get(month_num)

# def calculate_monthly_net_sales(order_amount: float, 
#                               billing_method: int,
#                               start_date: datetime.date,
#                               end_date: datetime.date) -> dict:
#     """Calculate monthly net sales with support for Japanese fiscal year (April-March)"""
#     if not all([order_amount, start_date, end_date]):
#         return {}
    
#     delta = relativedelta(end_date, start_date)
#     total_months = delta.years * 12 + delta.months + 1
#     billing_method = billing_method if billing_method else total_months
#     monthly_amount = order_amount / billing_method
    
#     result = {}
#     current_date = start_date
#     for _ in range(billing_method):
#         # Use both English and Japanese month names for compatibility
#         month_key = get_english_month_name(current_date.month)
#         result[month_key] = monthly_amount
#         current_date += relativedelta(months=1)
#     return result

def extract_project_rank(phase: str) -> str:
    """Extract project rank from phase with Japanese support"""
    if not phase:
        return "E"
    
    phase = phase.strip().upper()
    
    # Handle Japanese SA phase marking
    if phase.startswith("SA") or phase.startswith("受注"):
        return "SA"
    
    # Handle A-F phase markings
    if phase.startswith(("A", "B", "C", "D", "E", "F")):
        return phase[0]
    
    # Other Japanese phase mappings
    phase_mapping = {
        "受注内示": "A",
        "見込": "B", 
        "先方検討中": "C",
        "提案中": "D",
        "提案前商談中": "E",
        "初期コンタクト": "F"
    }
    
    # Look for Japanese phase names
    for jp_phase, rank in phase_mapping.items():
        if jp_phase in phase:
            return rank
            
    return "E"  # Default

# ------------------------
# API Endpoints (Updated for Japanese)
# ------------------------

@app.post("/api/upload/crm")
async def upload_crm(payload: CRMUploadPayload, db: Session = Depends(get_db)):
    new_upload = MonthlyUpload(
        upload_type="CRM",
        file_name=payload.file_name,
        name=payload.name,
        month=payload.month,
        year=payload.year,
        description=payload.description
    )
    db.add(new_upload)
    db.commit()
    db.refresh(new_upload)

    for rec in payload.records:
        # Convert Pydantic model to SQLAlchemy model
        raw_record = CRMProjectRaw(
            upload_id=new_upload.upload_id,
            project_id=rec.project_id,
            status=rec.status,
            phase=rec.phase,
            company_name=rec.company_name,
            department=rec.department,
            project_name=rec.project_name,
            project_manager=rec.project_manager,
            pm=rec.pm,
            order_amount_gross=rec.order_amount_gross,
            order_amount_net=rec.order_amount_net,
            unit=rec.unit,
            contract_start_date=parse_date(rec.contract_start_date),
            contract_end_date=parse_date(rec.contract_end_date),
            billing_method=rec.billing_method,
            high_potential_mark=convert_high_potential(rec.high_potential_mark or "")
        )
        db.add(raw_record)

    db.commit()
    return {"message": "CRMデータがアップロードされました", "upload_id": new_upload.upload_id}


@app.post("/api/upload/erp/sales")
async def upload_erp_sales(payload: ERPSalesUploadPayload, db: Session = Depends(get_db)):
    new_upload = MonthlyUpload(upload_type="ERP_Sales", 
        file_name=payload.file_name,
        name=payload.name,        
        month=payload.month,
        year=payload.year,
        description=payload.description)
    db.add(new_upload)
    db.commit()
    db.refresh(new_upload)

    for rec in payload.records:
        try:
            # Parse and validate sales date
            sales_date = parse_date(rec.sales_posting_date)
            
            # If date is still invalid, use current date as fallback
            if not sales_date:
                print(f"Invalid date for JOB {rec.job_no}: {rec.sales_posting_date}, using today's date")
                sales_date = datetime.now().date()
            
            # Create raw record
            raw_record = ERPSalesRaw(
                upload_id=new_upload.upload_id,
                job_no=rec.job_no,
                client_code=rec.client_code,
                client_name=rec.client_name,
                project_name=rec.project_name,
                sales_amount=rec.sales_amount or 0,
                operating_profit=rec.operating_profit or 0,
                sales_date=sales_date,
                progress_status=rec.progress
            )
            db.add(raw_record)
        except Exception as e:
            print(f"Error processing ERP record: {e}")
            # Optionally add to an errors list or continue
            
    db.commit()
    return {"message": "ERPデータがアップロードされました", "upload_id": new_upload.upload_id}


@app.post("/api/upload/datacode")
async def upload_datacode(payload: DataCodeUploadPayload, db: Session = Depends(get_db)):
    new_upload = MonthlyUpload(
        upload_type="DataCode",
        file_name=payload.file_name,
        name=payload.name,
        month=payload.month,
        year=payload.year,
        description=payload.description
    )
    db.add(new_upload)
    db.commit()
    db.refresh(new_upload)

    for rec in payload.records:
        raw_record = DataCodeRaw(
            upload_id=new_upload.upload_id,
            customer_name=rec.customer_name,
            department_name=rec.department_name,
            parent_code=rec.parent_code,
            project_name=rec.project_name
        )
        db.add(raw_record)
    db.commit()
    return {"message": "データコードがアップロードされました", "upload_id": new_upload.upload_id}


@app.get("/api/uploads/crm")
async def get_crm_uploads(db: Session = Depends(get_db)):
    uploads = db.query(MonthlyUpload).filter_by(upload_type="CRM").order_by(MonthlyUpload.upload_timestamp.desc()).all()
    return [
        {
            "upload_id": u.upload_id, 
            "file_name": u.file_name, 
            "name": u.name,
            "month": u.month,
            "year": u.year,
            "description": u.description,
            "timestamp": u.upload_timestamp.isoformat()
        }for u in uploads
    ]

@app.get("/api/report/{report_id}")
async def get_specific_report(report_id: int, db: Session = Depends(get_db)):
    report = db.query(PerformanceReportGenerationHistory).filter_by(report_id=report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="レポートが見つかりません")
    return {
        "report_id": report.report_id,
        "name": report.name,  # Add name
        "month": report.month,
        "year": report.year,
        "upload_id": report.upload_id,
        "generated_at": report.generated_timestamp.isoformat(),
        "report_snapshot": report.report_snapshot
    }

@app.get("/api/uploads/erp")
async def get_erp_uploads(db: Session = Depends(get_db)):
    uploads = db.query(MonthlyUpload).filter_by(upload_type="ERP_Sales").order_by(MonthlyUpload.upload_timestamp.desc()).all()
    return [
        {
            "upload_id": u.upload_id, 
            "file_name": u.file_name, 
            "name": u.name,
            "month": u.month,
            "year": u.year,
            "description": u.description,
            "timestamp": u.upload_timestamp.isoformat()
        }for u in uploads
    ]

@app.get("/api/uploads/datacode")
async def get_datacode_uploads(db: Session = Depends(get_db)):
    uploads = db.query(MonthlyUpload).filter_by(upload_type="DataCode").order_by(MonthlyUpload.upload_timestamp.desc()).all()
    return [
        {
            "upload_id": u.upload_id, 
            "file_name": u.file_name, 
            "name": u.name,
            "month": u.month,
            "year": u.year,
            "description": u.description,
            "timestamp": u.upload_timestamp.isoformat()
        }for u in uploads
    ]

@app.get("/api/reports")
async def get_all_reports(db: Session = Depends(get_db)):
    reports = db.query(PerformanceReportGenerationHistory).order_by(PerformanceReportGenerationHistory.generated_timestamp.desc()).all()
    return [
        {
            "report_id": r.report_id,
            "name": r.name, 
            "upload_id": r.upload_id,        
            "month": r.month,
            "year": r.year,
            "generated_at": r.generated_timestamp.isoformat()
        }for r in reports
    ]


@app.get("/api/uploads/crm/{upload_id}")
async def get_specific_crm_upload(upload_id: int, db: Session = Depends(get_db)):
    crm_data = db.query(CRMProjectRaw).filter_by(upload_id=upload_id).all()
    if not crm_data:
        raise HTTPException(status_code=404, detail="CRMアップロードが見つかりません")
    return {
        "upload_id": upload_id,
        "records": [record.__dict__ for record in crm_data]
    }

@app.get("/api/uploads/erp/{upload_id}")
async def get_specific_erp_upload(upload_id: int, db: Session = Depends(get_db)):
    erp_data = db.query(ERPSalesRaw).filter_by(upload_id=upload_id).all()
    if not erp_data:
        raise HTTPException(status_code=404, detail="ERPアップロードが見つかりません")
    return {
        "upload_id": upload_id,
        "records": [record.__dict__ for record in erp_data]
    }

@app.get("/api/uploads/datacode/{upload_id}")
async def get_specific_datacode_upload(upload_id: int, db: Session = Depends(get_db)):
    datacode_data = db.query(DataCodeRaw).filter_by(upload_id=upload_id).all()
    if not datacode_data:
        raise HTTPException(status_code=404, detail="データコードアップロードが見つかりません")
    return {
        "upload_id": upload_id,
        "records": [record.__dict__ for record in datacode_data]
    }

@app.post("/api/generate_report")
async def generate_performance_report_endpoint(request: ReportRequest, db: Session = Depends(get_db)):
    print(f"Received report request with IDs: 1 {request.upload_ids}")
    
    # Validate input contains exactly 3 unique IDs
    if len(request.upload_ids) != 3 or len(set(request.upload_ids)) != 3:
        raise HTTPException(
            status_code=400,
            detail="正確に3つの異なるアップロードIDを指定してください CRM、ERP売上、データコードの各タイプから1つずつ "
        )

    try:
        # Get all specified upload records
        print(f"Received report request with IDs: 2 {request.upload_ids}")

        uploads = db.query(MonthlyUpload).filter(
            MonthlyUpload.upload_id.in_(request.upload_ids)
        ).all()

        # Verify we found exactly 3 records
        if len(uploads) != 3:
            missing_ids = set(request.upload_ids) - {u.upload_id for u in uploads}
            raise ValueError(f"次のアップロードIDが見つかりません: {', '.join(map(str, missing_ids))}")
        print(f"Received report request with IDs:3 {request.upload_ids}")

        # Categorize uploads by type
        type_mapping = {u.upload_type: u for u in uploads}
        print(f"Received report request with IDs: 4 {request.upload_ids}")
        
        # Validate type composition
        required_types = {"CRM", "ERP_Sales", "DataCode"}
        found_types = set(type_mapping.keys())
        print(f"Received report request with IDs: 5 {request.upload_ids}")
        
        if found_types != required_types:
            missing = required_types - found_types
            extra = found_types - required_types
            error_msg = []
            if missing:
                error_msg.append(f"不足しているタイプ: {', '.join(missing)}")
            if extra:
                error_msg.append(f"不要なタイプ: {', '.join(extra)}")
            raise ValueError("アップロードタイプが不正です。 " + "; ".join(error_msg))
        print(f"Received report request with IDs:  6 {request.upload_ids}")

        # Assign validated uploads
        crm_upload = type_mapping["CRM"]
        erp_upload = type_mapping["ERP_Sales"]
        datacode_upload = type_mapping["DataCode"]
        print(f"Received report request with IDs: 7 {request.upload_ids}")

        # Fetch corresponding data
        crm_data = db.query(CRMProjectRaw).filter_by(upload_id=crm_upload.upload_id).all()
        erp_data = db.query(ERPSalesRaw).filter_by(upload_id=erp_upload.upload_id).all()
        datacode_data = db.query(DataCodeRaw).filter_by(upload_id=datacode_upload.upload_id).all()
        print(f"Received report request with IDs: 8 {request.upload_ids}")

        # Validate data existence
        if not crm_data:
            raise ValueError(f"CRMデータが存在しません（アップロードID: {crm_upload.upload_id}）")
        if not erp_data:
            raise ValueError(f"ERPデータが存在しません（アップロードID: {erp_upload.upload_id}）")
        if not datacode_data:
            raise ValueError(f"データコードデータが存在しません（アップロードID: {datacode_upload.upload_id}）")
        print(f"Received report request with IDs: 9 {request.upload_ids}")

        crm_data = db.query(CRMProjectRaw).filter_by(upload_id=crm_upload.upload_id).all()
        erp_data = db.query(ERPSalesRaw).filter_by(upload_id=erp_upload.upload_id).all()
        datacode_data = db.query(DataCodeRaw).filter_by(upload_id=datacode_upload.upload_id).all()
        print(f"Received report request with IDs: 10 {request.upload_ids}")

        if not crm_data:
            raise ValueError("CRMデータが見つかりません")
        if not erp_data:
            raise ValueError("ERPデータが見つかりません")
        if not datacode_data:
            raise ValueError("データコードデータが見つかりません")
        print(f"Received report request with IDs: 11 {request.upload_ids}")

        # Generate performance report
        report = create_performance_report(
            [d.__dict__ for d in erp_data],
            [d.__dict__ for d in datacode_data],
            [d.__dict__ for d in crm_data],
            crm_month=crm_upload.month,
            crm_year=crm_upload.year
        )
        print(f"Received report request with IDs: 12 {request.upload_ids}")

        # Save report with all upload IDs reference
        # In the generate_performance_report_endpoint function, modify the report saving section:

        # Save report with all upload IDs reference
        new_report = PerformanceReportGenerationHistory(
            report_snapshot=report,
            upload_id=erp_upload.upload_id,  # Use one of the upload IDs (ERP in this case)
            name=request.name,
            month=crm_upload.month,  # Use CRM upload's month/year
            year=crm_upload.year,
            generated_timestamp=func.now()
        )
        print(f"Received report request with IDs: 13 {request.upload_ids}")
        
        db.add(new_report)
        db.commit()
        db.refresh(new_report)
        print(f"Received report request with IDs: 14 {request.upload_ids}")


        return {
            "message": "レポートが生成されました",
            "report_id": new_report.report_id,
            "name": new_report.name, 
            "generated_at": new_report.generated_timestamp.isoformat(),
            "report_snapshot": new_report.report_snapshot
        }

    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="内部エラーが発生しました")

# def create_performance_report(zac_data, datacode_data, kintone_data):
#     """Create performance report with support for Japanese field names and calendar order"""
#     performance_report = {}
    
#     # Define Japanese month names in calendar order (January to December)
#     jp_months = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"]
#     eng_months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    
#     # Build phase to project rank mapping
#     phase_rank_mapping = {}
#     for item in kintone_data:
#         phase = item.get('phase', '')
#         phase_rank_mapping[item['project_name']] = extract_project_rank(phase)

#     # Process ERP data
#     for item in zac_data:
#         project_name = item.get('project_name')
#         if not project_name:
#             continue
            
#         project_code = f"{int(item['job_no']):07d}" if item['job_no'].isdigit() else item['job_no']
#         if project_code not in performance_report:
#             parent_code = next(
#                 (x['parent_code'] for x in datacode_data 
#                  if x.get('project_name') == project_name),
#                 ""
#             )
#             performance_report[project_code] = {
#                 "親コード": parent_code,
#                 "顧客名": item.get('client_name', ''),
#                 "案件名": project_name,
#                 "案件ランク": 'SA',
#                 "案件コード": project_code,
#                 **{month: 0 for month in eng_months},  # Keep English month names for internal calculations
#                 "純売上額": 0
#             }

#         # In the create_performance_report function, modify the part where it processes sales dates:

#         if item.get('sales_date'):
#             try:
#                 month = item['sales_date'].strftime('%B')  # English month name
#                 op_profit = float(item.get('operating_profit', 0))
#                 performance_report[project_code][month] += op_profit
#                 performance_report[project_code]["純売上額"] += op_profit
#             except Exception as e:
#                 print(f"Error processing ERP data with date {item.get('sales_date')}: {e}")
#                 # Use current month as fallback
#                 current_month = datetime.now().strftime('%B')
#                 op_profit = float(item.get('operating_profit', 0))
#                 performance_report[project_code][current_month] += op_profit
#                 performance_report[project_code]["純売上額"] += op_profit
#         else:
#             # No sales date, assign to current month
#             current_month = datetime.now().strftime('%B')
#             op_profit = float(item.get('operating_profit', 0))
#             performance_report[project_code][current_month] += op_profit
#             performance_report[project_code]["純売上額"] += op_profit

#     # Process CRM data
#     for item in kintone_data:
#         try:
#             project_code = f"{item['project_id']:07d}"
#             high_potential = item.get('high_potential_mark', False)
#             project_rank = phase_rank_mapping.get(item['project_name'], 'E')

#             if (high_potential and project_rank in ['B', 'C', 'D', 'E', 'F']) \
#                or project_rank == 'A':

#                 if project_code not in performance_report:
#                     parent_code = next(
#                         (x['parent_code'] for x in datacode_data 
#                          if x.get('project_name') == item['project_name']),
#                         ""
#                     )
#                     if project_rank == "SA":
#                         mapped_rank = "SA"
#                     elif project_rank == "A":
#                         mapped_rank = "A"
#                     elif project_rank in ["B", "C", "D"]:
#                         mapped_rank = "B"
#                     elif project_rank == "E":
#                         mapped_rank = "C"
#                     elif project_rank == "F":
#                         mapped_rank = "D"
#                     else:  # default case
#                         mapped_rank = "E"
#                     performance_report[project_code] = {
#                         "親コード": parent_code,
#                         "顧客名": item.get('company_name', ''),
#                         "案件名": item['project_name'],
#                         "案件ランク": mapped_rank,
#                         "案件コード": project_code,
#                         **{month: 0 for month in eng_months},  # Keep English month names
#                         "純売上額": 0
#                     }
                    
#                 monthly_sales = calculate_monthly_net_sales(
#                     float(item.get('order_amount_net', 0)) * 1000000,
#                     item.get('billing_method'),
#                     item.get('contract_start_date'),
#                     item.get('contract_end_date')
#                 )

#                 for month, amount in monthly_sales.items():
#                     performance_report[project_code][month] += amount
#                     performance_report[project_code]["純売上額"] += amount
#         except Exception as e:
#             print(f"Error processing CRM data: {e}")

#     # Convert to list of values and reorganize fields with months in calendar order and 純売上額 at the end
#     report_list = []
#     for project_code, data in performance_report.items():
#         # Create new properly ordered dictionary
#         ordered_data = {
#             "親コード": data["親コード"],
#             "顧客名": data["顧客名"],
#             "案件名": data["案件名"],
#             "案件ランク": data["案件ランク"],
#             "案件コード": data["案件コード"]
#         }
        
#         # Add months in calendar order (January through December)
#         for i, eng_month in enumerate(eng_months):
#             jp_month = jp_months[i]
#             ordered_data[jp_month] = data[eng_month]
        
#         # Add 純売上額 at the end
#         ordered_data["純売上額"] = data["純売上額"]
        
#         report_list.append(ordered_data)
    
#     return report_list

def get_financial_year_dates(month_str: str, year_str: str) -> tuple:
    """Calculate financial year dates based on Japanese fiscal calendar"""
    jp_month_map = {
        "4月": 4, "5月": 5, "6月": 6, "7月": 7, "8月": 8, "9月": 9,
        "10月": 10, "11月": 11, "12月": 12, "1月": 1, "2月": 2, "3月": 3
    }
    
    # Clean input and get month number
    clean_month = month_str.strip()
    month_num = jp_month_map.get(clean_month, 4)  # Default to April if invalid
    
    base_year = int(year_str)
    
    # Japanese fiscal year starts in April
    if month_num >= 4:  # April-March
        start_year = base_year
    else:  # January-March belong to previous fiscal year
        start_year = base_year - 1
    
    return (
        datetime(start_year, 4, 1).date(),
        datetime(start_year + 1, 3, 31).date()
    )

def get_fiscal_year(report_month: str, report_year: str) -> tuple:
    """Determine fiscal year based on Japanese fiscal calendar (April-March)"""
    try:
        # Convert report month to numerical value
        month_num = datetime.strptime(report_month, "%B").month
        year = int(report_year)
        
        if month_num >= 4:  # April or later
            # Fiscal year starts in current report year
            return (datetime(year, 4, 1).date(), datetime(year+1, 3, 31).date())
        else:  # January-March
            # Fiscal year started in previous year
            return (datetime(year-1, 4, 1).date(), datetime(year, 3, 31).date())
            
    except ValueError:
        raise ValueError(f"Invalid month format: {report_month}")

def create_performance_report(zac_data, datacode_data, kintone_data, crm_month: str, crm_year: str):
    """Create performance report with detailed logging"""
    performance_report = {}
    erp_projects = {}
    crm_projects = {}
    print(f"Starting report generation for {crm_month}/{crm_year}")
    
    try:
        financial_year_start, financial_year_end = get_fiscal_year(crm_month, crm_year)
        print(f"Financial year range: {financial_year_start} to {financial_year_end}")
        
        # Japanese month names in fiscal order
        jp_months = ["4月", "5月", "6月", "7月", "8月", "9月", 
                    "10月", "11月", "12月", "1月", "2月", "3月"]
        
        # Process ERP data with detailed logging
        print(f"Processing {len(zac_data)} ERP records")
        for idx, item in enumerate(zac_data):
            try:
                print(f"Processing ERP item {idx}: {item.get('job_no')}")
                
                # Financial year validation
                if not item.get('sales_date'):
                    print(f"Skipping ERP item {idx} - Missing sales_date")
                    continue
                    
                if not (financial_year_start <= item['sales_date'] <= financial_year_end):
                    print(f"Skipping ERP item {idx} - Date {item['sales_date']} outside financial year")
                    continue

                project_name = item.get('project_name')
                if not project_name:
                    print(f"Skipping ERP item {idx} - Missing project_name")
                    continue
                
                # Parent code resolution logging
                parent_code = next(
                    (x['parent_code'] for x in datacode_data 
                     if x.get('project_name') == project_name),
                    None
                )
                if not parent_code:
                    print(f"Using client name as parent code for {project_name}")
                    parent_code = item.get('client_name', '-')

                project_code = f"{int(item['job_no']):07d}" if item['job_no'].isdigit() else item['job_no']
                if project_code not in erp_projects:
                    print(f"Creating new ERP project {project_code} - {project_name}")
                    erp_projects[project_code] = {
                        "親コード": parent_code,
                        "顧客名": item.get('client_name', ''),
                        "案件名": project_name,
                        "案件ランク": 'SA',
                        "案件コード": project_code,
                        **{month: 0 for month in jp_months},
                        "純売上額": 0
                    }

                # Sales processing logging
                try:
                    op_profit = float(item.get('operating_profit', 0))
                    sales_month = item['sales_date'].month
                    fiscal_month_index = (sales_month - 4) % 12
                    jp_month = jp_months[fiscal_month_index]
                    
                    print(f"Adding {op_profit} to {project_code} for {jp_month}")
                    erp_projects[project_code][jp_month] += op_profit
                    erp_projects[project_code]["純売上額"] += op_profit
                    
                except Exception as e:
                    print(f"Error processing ERP sales data {item}: {str(e)}")
                    
            except Exception as e:
                print(f"Failed processing ERP item {idx}: {str(e)}")

        # Process CRM data with detailed logging
        print(f"Processing {len(kintone_data)} CRM records")
        for idx, item in enumerate(kintone_data):
            try:
                print(f"Processing CRM item {idx}: {item.get('project_id')}")
                
                project_code = f"{item['project_id']:07d}"
                high_potential = item.get('high_potential_mark', False)
                project_rank = extract_project_rank(item.get('phase', ''))
                
                # Eligibility check logging
                if not ((high_potential and project_rank in ['B', 'C', 'D', 'E', 'F']) or project_rank == 'A'):
                    print(f"Skipping CRM item {idx} - Not eligible (Rank: {project_rank}, High Potential: {high_potential})")
                    continue

                monthly_sales = calculate_monthly_net_sales(
                    float(item.get('order_amount_net', 0)) * 1000000,
                    item.get('billing_method', 1),
                    item.get('contract_start_date'),
                    item.get('contract_end_date')
                )
                
                # Financial year validation logging
                valid_months = [m for m in monthly_sales if financial_year_start <= m <= financial_year_end]
                if not valid_months:
                    print(f"Skipping CRM project {project_code} - No sales in financial year")
                    continue

                # Parent code resolution logging
                parent_code = next(
                    (x['parent_code'] for x in datacode_data 
                     if x.get('project_name') == item['project_name']),
                    None
                )
                if not parent_code:
                    print(f"Using company name as parent code for {item['project_name']}")
                    parent_code = item.get('company_name', '-')

                # Rank mapping logging
                rank_map = {"SA": "SA", "A": "A", "B": "B", "C": "B", "D": "B", "E": "C", "F": "D"}
                mapped_rank = rank_map.get(project_rank, "E")
                print(f"Mapped rank {project_rank} -> {mapped_rank} for {project_code}")

                if project_code not in crm_projects:
                    print(f"Creating new CRM project {project_code} - {item['project_name']}")
                    crm_projects[project_code] = {
                        "親コード": parent_code,
                        "顧客名": item.get('company_name', ''),
                        "案件名": item['project_name'],
                        "案件ランク": mapped_rank,
                        "案件コード": project_code,
                        **{month: 0 for month in jp_months},
                        "純売上額": 0
                    }

                # Sales distribution logging
                for month_date, amount in monthly_sales.items():
                    if financial_year_start <= month_date <= financial_year_end:
                        fiscal_month_index = (month_date.month - 4) % 12
                        jp_month = jp_months[fiscal_month_index]
                        print(f"Adding {amount} to {project_code} for {jp_month}")
                        crm_projects[project_code][jp_month] += amount
                        crm_projects[project_code]["純売上額"] += amount
                        
            except Exception as e:
                print(f"Failed processing CRM item {idx}: {str(e)}")

        print(f"Report generation completed. Total projects: {len(crm_projects) + len(erp_projects)}")
        return list(erp_projects.values()) + list(crm_projects.values())
        
    except Exception as e:
        print(f"Report generation failed: {str(e)}")
        raise

def calculate_monthly_net_sales(order_amount: float, 
                              billing_method: int,
                              start_date: date,
                              end_date: date) -> dict:
    """Calculate monthly sales with logging"""
    print(f"Calculating monthly sales for contract {start_date} to {end_date}")
    
    monthly_sales = {}
    try:
        if not all([order_amount, start_date, end_date]):
            print("Invalid inputs for monthly sales calculation")
            return monthly_sales
            
        delta = relativedelta(end_date, start_date)
        total_months = delta.years * 12 + delta.months + 1
        billing_method = billing_method if billing_method else total_months
        monthly_amount = order_amount / billing_method
        
        print(f"Contract duration: {total_months} months, Billing: {billing_method} payments")
        print(f"Monthly amount: {monthly_amount}")

        current_date = start_date
        for i in range(billing_method):
            month_key = current_date.replace(day=1)
            monthly_sales[month_key] = monthly_amount
            print(f"Month {i+1}: {month_key} - {monthly_amount}")
            current_date += relativedelta(months=1)
            
    except Exception as e:
        print(f"Failed calculating monthly sales: {str(e)}")
    
    return monthly_sales


@app.get("/api/latest_report")
async def get_latest_report(db: Session = Depends(get_db)):
    latest_report = db.query(PerformanceReportGenerationHistory).order_by(
        PerformanceReportGenerationHistory.generated_timestamp.desc()
    ).first()
    
    if not latest_report:
        raise HTTPException(status_code=404, detail="レポートが見つかりません")
    
    return {
        "report_id": latest_report.report_id,
        "upload_id": latest_report.upload_id,
        "name": latest_report.name,  # Add name to the response
        "month": latest_report.month,
        "year": latest_report.year,
        "generated_at": latest_report.generated_timestamp.isoformat(),
        "report_snapshot": latest_report.report_snapshot
    }   


@app.post("/api/generate_latest_report")
async def generate_latest_report(
    db: Session = Depends(get_db),
    name: str = Query("Latest Report", description="Name of the report")  # Default name if not provided
):
    # Get the latest uploaded data of each type
    latest_crm_upload = db.query(MonthlyUpload).filter_by(upload_type="CRM").order_by(MonthlyUpload.upload_timestamp.desc()).first()
    latest_erp_upload = db.query(MonthlyUpload).filter_by(upload_type="ERP_Sales").order_by(MonthlyUpload.upload_timestamp.desc()).first()
    latest_datacode_upload = db.query(MonthlyUpload).filter_by(upload_type="DataCode").order_by(MonthlyUpload.upload_timestamp.desc()).first()
    
    if not all([latest_crm_upload, latest_erp_upload, latest_datacode_upload]):
        raise HTTPException(status_code=400, detail="最新のアップロードデータが見つかりません。すべてのデータタイプをアップロードしてください。")
    
    # Get the data from the latest uploads
    crm_data = db.query(CRMProjectRaw).filter_by(upload_id=latest_crm_upload.upload_id).all()
    erp_data = db.query(ERPSalesRaw).filter_by(upload_id=latest_erp_upload.upload_id).all()
    datacode_data = db.query(DataCodeRaw).filter_by(upload_id=latest_datacode_upload.upload_id).all()

    # Generate performance report
    report = create_performance_report(
        [d.__dict__ for d in erp_data],
        [d.__dict__ for d in datacode_data],
        [d.__dict__ for d in crm_data],
        crm_month=latest_crm_upload.month,
        crm_year=latest_crm_upload.year
    )

    # Save report snapshot with the provided name
    new_report = PerformanceReportGenerationHistory(
        report_snapshot=report,
        upload_id=latest_erp_upload.upload_id,  # Just use one of the latest upload IDs for reference
        name=name,  # Save the name from the query parameter
        month=latest_crm_upload.month,
        year=latest_crm_upload.year,
        generated_timestamp=func.now()  # Explicitly set timestamp
    )
    db.add(new_report)
    db.commit()
    db.refresh(new_report)  # Refresh to get the latest data

    return {
        "message": "レポートが生成されました",
        "report_id": new_report.report_id,
        "name": new_report.name,  # Return the name in the response
        "month": new_report.month,
        "year": new_report.year,
        "generated_at": new_report.generated_timestamp.isoformat(),
        "report_snapshot": new_report.report_snapshot
    }


@app.post("/api/chat")
async def handle_chat_report_generation(
    request: ChatRequest, 
    db: Session = Depends(get_db)
):
    """
    Endpoint to handle chat-based report generation through Lyzr AI
    """
    # Generate or validate session ID
    session_id = request.session_id or str(uuid.uuid4())
    
    try:
        # Call Lyzr AI API
        print("Payload", request.message)
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                "https://agent-prod.studio.lyzr.ai/v3/inference/chat/",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": LYZR_API_KEY
                },
                json={
                    "user_id": "pranav@lyzr.ai",  # Replace with dynamic user ID in production
                    "agent_id": LYZR_AGENT_ID,
                    "session_id": session_id,
                    "message": request.message
                }
            )
            print("response",response)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"AI service error: {response.text}"
                )

            response_data = response.json()
            print("response _Data", response_data);
            # Extract and parse the report from the response
            report_text = response_data.get("response", "")
            report_data = parse_report_from_response(report_text)

            # Validate report structure
            if not validate_report_structure(report_data):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid report format received from AI service"
                )

            # Store the generated report with the provided name
            upload_record, report_record = store_generated_report(
                db=db,
                report_data=report_data,
                session_id=session_id,
                name=request.name,  # Pass the name from the payload
            )
            print("values",report_record.month, report_record.year)
            return {
                "session_id": session_id,
                "report_id": report_record.report_id,
                "upload_id": upload_record.upload_id,
                "name": report_record.name,  # Return the name in the response
                "month": report_record.month,
                "year": report_record.year,
                "generated_at": report_record.generated_timestamp.isoformat(),
                "report_data": report_data
            }

    except HTTPException as he:
        print("error",he)
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing chat request: {str(e)}"
        )

def parse_report_from_response(report_text: str) -> List[dict]:
    """
    Parse the report from the AI response text
    """
    try:
        # Extract JSON from markdown code blocks if present
        print("parse reponse function",report_text)
        json_str = report_text.split("```json")[1].split("```")[0].strip()
        print("json_str",json_str)
        return json.loads(json_str)
    except (IndexError, json.JSONDecodeError):
        # Fallback to direct JSON parsing
        try:
            return json.loads(report_text)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=500,
                detail="Could not parse report from AI response"
            )

def validate_report_structure(report_data: List[dict]) -> bool:
    """
    Validate the structure of the generated report
    """
    required_fields = [
        "親コード", "顧客名", "案件名", 
        "案件ランク", "案件コード", "純売上額"
    ]
    
    if not isinstance(report_data, list):
        return False
        
    for item in report_data:
        if not all(field in item for field in required_fields):
            return False
            
        if not isinstance(item.get("純売上額"), (int, float)):
            return False
            
    return True

def store_generated_report(
    db: Session,
    report_data: List[dict],
    session_id: str,
    name: str = "Chat Generated Report"  # Default name if not provided
) -> tuple:
    """
    Store the generated report in the database
    """
    try:
        name = name or "Chat Generated Report"
        latest_crm_upload = db.query(MonthlyUpload).filter_by(upload_type="CRM").order_by(MonthlyUpload.upload_timestamp.desc()).first()
        print("month and crm value",)
        # Create upload record
        upload_record = MonthlyUpload(
            upload_type="CHAT_REPORT",
            file_name=f"{name} - {datetime.now().isoformat()}",
            name=name,
            month=latest_crm_upload.month,
            year=latest_crm_upload.year,
            upload_timestamp=func.now()
        )
        db.add(upload_record)
        db.commit()
        db.refresh(upload_record)

        # Create report history record with the provided name
        report_record = PerformanceReportGenerationHistory(
            report_snapshot=report_data,
            upload_id=upload_record.upload_id,
            name=name,  # Use the provided name
            month=latest_crm_upload.month,
            year=latest_crm_upload.year,
            generated_timestamp=func.now()
        )
        db.add(report_record)
        db.commit()
        db.refresh(report_record)

        return upload_record, report_record

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database storage failed: {str(e)}"
        )
    


@app.post("/api/compare_reports")
async def compare_reports(request: ReportComparisonRequest, db: Session = Depends(get_db)):
    # Generate a session ID if not provided
    session_id = request.session_id or str(uuid.uuid4())
    print("parameter",request);
    # Create a new comparison record
    comparison_id = str(uuid.uuid4())  # Generate a unique comparison ID
    new_comparison = ReportComparison(
        comparison_id=comparison_id,
        session_id=session_id,
        query_text=request.query,
        old_report_size=len(request.old_report),
        new_report_size=len(request.new_report),
        status="pending",
        result=None,
        error=None
    )
    
    # Save the initial record
    db.add(new_comparison)
    db.commit()

    # LYZR AI API details
    lyzr_api_url = "https://agent-prod.studio.lyzr.ai/v3/inference/chat/"
    lyzr_api_key = LYZR_API_KEY  # Store this in environment variables in production
    
    # Prepare the message with context about the reports
    message = f"""
        I need to analyze two sales reports. Here's my query: {request.query}

        First report (older):
        {json.dumps(request.old_report, ensure_ascii=False)}

        Second report (newer):
        {json.dumps(request.new_report, ensure_ascii=False)}

        Please analyze the differences between these reports, focusing on changes in projects, revenue, rankings, and trends.
        """
    print("message",message)
    try:
        # Make the API request to LYZR AI
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                lyzr_api_url,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": lyzr_api_key
                },
                json={
                    "user_id": "pranav@lyzr.ai",  
                    "agent_id": LYZR_COMPARE_AGENT_ID,
                    "session_id": session_id,
                    "message": message
                }
            )
            print("response code",response)
            print("response text",response.text)
            # Check if the request was successful
            if response.status_code == 200:
                response_data = response.json()
                
                # Store the result for future reference
                comparison_results[comparison_id] = {
                    "timestamp": datetime.now().isoformat(),
                    "query": request.query,
                    "session_id": session_id,
                    "result": response_data.get("response", "No response received"),
                    "old_report_size": len(request.old_report),
                    "new_report_size": len(request.new_report)
                }
                db.query(ReportComparison).filter_by(comparison_id=comparison_id).update({
                    "status": "success",
                    "result": response_data.get("response", "No response received")
                })
                db.commit()
                return {
                    "comparison_id": comparison_id,
                    "status": "success",
                    "result": response_data.get("response", "No response received")
                }
            else:
                return {
                    "comparison_id": None,
                    "status": "error",
                    "error": f"API request failed with status code {response.status_code}: {response.text}"
                }
                
    except Exception as e:
        return {
            "comparison_id": None,
            "status": "error",
            "error": f"Error processing request: {str(e)}"
        }

# Add an endpoint to retrieve previous comparison results
@app.get("/api/comparison/{comparison_id}")
async def get_comparison_result(comparison_id: str, db: Session = Depends(get_db)):
    comparison = db.query(ReportComparison).filter_by(comparison_id=comparison_id).first()
    if not comparison:
        raise HTTPException(status_code=404, detail="比較結果が見つかりません")
    
    return comparison

@app.get("/api/comparisons/session/{session_id}")
async def list_session_comparisons(session_id: str, db: Session = Depends(get_db)):
    comparisons = db.query(ReportComparison).filter_by(session_id=session_id).order_by(
        ReportComparison.created_at.desc()
    ).all()
    
    return [
        {
            "comparison_id": comp.comparison_id,
            "query_text": comp.query_text,
            "status": comp.status,
            "created_at": comp.created_at.isoformat()
        }
        for comp in comparisons
    ]



# Add an endpoint to list all comparisons
@app.get("/api/comparisons")
async def list_all_comparisons(db: Session = Depends(get_db)):
    comparisons = db.query(ReportComparison).order_by(
        ReportComparison.created_at.desc()
    ).all()
    
    return [
        {
            "comparison_id": comp.comparison_id,
            "session_id": comp.session_id,
            "query_text": comp.query_text,
            "status": comp.status,
            "created_at": comp.created_at.isoformat()
        }
        for comp in comparisons
    ]

# Add an endpoint for follow-up questions on the same reports
@app.post("/api/comparison/follow_up")
async def comparison_follow_up(
    request: ReportComparisonRequest, 
    db: Session = Depends(get_db)
):
    # This uses the same session_id but creates a new comparison record
    # allowing for conversation history while maintaining separate records
    
    
    # Ensure session_id exists
    if not request.session_id:
        raise HTTPException(status_code=400, detail="Session ID required")

    prev_comparisons = db.query(ReportComparison).filter_by(
        session_id=request.session_id
    ).order_by(ReportComparison.created_at.asc()).all()

    # Create a new comparison record
    comparison_id = str(uuid.uuid4())
    new_comparison = ReportComparison(
        comparison_id=comparison_id,
        session_id=request.session_id,
        query_text=request.query,
        old_report_size=len(request.old_report),
        new_report_size=len(request.new_report),
        status="pending",
        result=None,
        error=None
    )
    
    db.add(new_comparison)
    db.commit()
    
    # LYZR AI API details
    lyzr_api_url = "https://agent-prod.studio.lyzr.ai/v3/inference/chat/"
    lyzr_api_key = LYZR_API_KEY
    
    # For follow-up questions, we use the same session_id to maintain conversation context

    history = "\n".join(
        [f"Q: {comp.query_text}\nA: {comp.result}" 
         for comp in prev_comparisons if comp.result]
    )
    
    message = f"""Conversation History:
    {history}
        Follow-up question about the reports: {request.query}

        Please analyze the reports again with this specific focus.
        """
    
    try:
        # Make the API request to LYZR AI
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                lyzr_api_url,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": lyzr_api_key
                },
                json={
                    "user_id": "api_user@example.com",
                    "agent_id": LYZR_COMPARE_AGENT_ID,
                    "session_id": request.session_id,
                    "message": message
                }
            )
            
            # Process response similar to the main endpoint
            if response.status_code == 200:
                response_data = response.json()
                result = response_data.get("response", "No response received")
                
                db.query(ReportComparison).filter_by(comparison_id=comparison_id).update({
                    "status": "success",
                    "result": result
                })
                db.commit()
                
                return {
                    "comparison_id": comparison_id,
                    "status": "success",
                    "result": result,
                    "session_id": request.session_id
                }
            else:
                error_msg = f"API request failed with status code {response.status_code}: {response.text}"
                
                db.query(ReportComparison).filter_by(comparison_id=comparison_id).update({
                    "status": "error",
                    "error": error_msg
                })
                db.commit()
                
                return {
                    "comparison_id": comparison_id,
                    "status": "error",
                    "error": error_msg,
                    "session_id": request.session_id
                }
                
    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        
        db.query(ReportComparison).filter_by(comparison_id=comparison_id).update({
            "status": "error",
            "error": error_msg
        })
        db.commit()
        
        return {
            "comparison_id": comparison_id,
            "status": "error",
            "error": error_msg,
            "session_id": request.session_id
        }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)


@app.get("/api/latest_uploads")
async def get_latest_uploads(db: Session = Depends(get_db)):
    """Get metadata and data for latest uploads of each type"""
    def get_latest(upload_type: str):
        return db.query(MonthlyUpload).filter_by(upload_type=upload_type)\
            .order_by(MonthlyUpload.upload_timestamp.desc()).first()

    def get_upload_data(upload_type: str, upload_id: int):
        if upload_type == "CRM":
            return db.query(CRMProjectRaw).filter_by(upload_id=upload_id).all()
        elif upload_type == "ERP_Sales":
            return db.query(ERPSalesRaw).filter_by(upload_id=upload_id).all()
        elif upload_type == "DataCode":
            return db.query(DataCodeRaw).filter_by(upload_id=upload_id).all()
        return []

    def format_upload(upload: Optional[MonthlyUpload]) -> Optional[dict]:
        """Format upload record with data"""
        if not upload:
            return None
            
        data = get_upload_data(upload.upload_type, upload.upload_id)
        return {
            "name": upload.name,
            "timestamp": upload.upload_timestamp.isoformat(),
            "upload_id": upload.upload_id,
            "data": [item.__dict__ for item in data]
        }

    return {
        "crm": format_upload(get_latest("CRM")),
        "erp": format_upload(get_latest("ERP_Sales")),
        "datacode": format_upload(get_latest("DataCode"))
    }

def format_upload(upload: Optional[MonthlyUpload]) -> Optional[dict]:
    """Format upload record for response"""
    if not upload:
        return None
    return {
        "name": upload.name,
        "timestamp": upload.upload_timestamp.isoformat(),
        "upload_id": upload.upload_id
    }


def alter_tables():
    session = SessionLocal()
    try:
        # Add month and year columns to monthly_uploads table if they don't exist
        session.execute(text("""
            ALTER TABLE monthly_uploads 
            ADD COLUMN IF NOT EXISTS month VARCHAR(20),
            ADD COLUMN IF NOT EXISTS year VARCHAR(4)
        """))

        # Add month and year columns to performance_report_generation_history table if they don't exist
        session.execute(text("""
            ALTER TABLE performance_report_generation_history 
            ADD COLUMN IF NOT EXISTS month VARCHAR(20),
            ADD COLUMN IF NOT EXISTS year VARCHAR(4)
        """))

        session.commit()
        print("Successfully altered tables to add month and year columns.")
    except Exception as e:
        session.rollback()
        print(f"Error altering tables: {e}")
    finally:
        session.close()

def update_existing_records():
    session = SessionLocal()
    try:
        # Update records with default values
        session.execute(text("""
            UPDATE monthly_uploads
            SET month = 'September'
            WHERE month IS NULL
        """))
        session.execute(text("""
            UPDATE monthly_uploads
            SET year = '2024'
            WHERE year IS NULL
        """))

        session.execute(text("""
            UPDATE performance_report_generation_history
            SET month = 'September'
            WHERE month IS NULL
        """))
        session.execute(text("""
            UPDATE performance_report_generation_history
            SET year = '2024'
            WHERE year IS NULL
        """))

        session.commit()
        print("Successfully updated existing records.")
    except Exception as e:
        session.rollback()
        print(f"Error updating records: {e}")
    finally:
        session.close()

@app.post("/api/update_records")
def api_update_existing_records():
    """
    API to alter tables and update existing records.
    """
    try:
        alter_tables()  # Ensure columns exist before updating
        update_existing_records()
        return {"message": "Tables altered and records updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating records: {str(e)}")


@app.get("/api/monthly_uploads")
def get_monthly_uploads(db: Session = Depends(lambda: SessionLocal())):
    uploads = db.query(MonthlyUpload).all()
    return uploads

@app.get("/api/reports")
def get_reports(db: Session = Depends(lambda: SessionLocal())):
    reports = db.query(PerformanceReportGenerationHistory).all()
    return reports


@app.get("/api/table_structure")
def get_table_structure():
    inspector = inspect(engine)
    structure = {}
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        structure[table_name] = {col['name']: str(col['type']) for col in columns}
    return structure

@app.get("/health")
def health_check():
    return {"status": "healthy"}