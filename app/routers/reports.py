from datetime import date, timedelta
from io import BytesIO
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import get_db
from app.services.payroll import PayrollService
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

router = APIRouter(
    prefix="/reports",
    tags=["reports"]
)

@router.get("/export/payroll")
async def export_payroll(
    start_date: date = Query(None),
    end_date: date = Query(None),
    branch_id: int = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """
    Generate and download Global Payroll Excel Report.
    """
    if not start_date:
        today = date.today()
        start_date = today.replace(day=1)
    if not end_date:
        today = date.today()
        # End of current month approx logic or just today
        if today.month == 12:
            end_date = date(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(today.year, today.month + 1, 1) - timedelta(days=1)

    # 1. Get Data
    payroll_data = await PayrollService.get_payroll_stats(db, start_date, end_date, branch_id)
    
    # 2. Create Excel
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "État de Paie"
    
    # Headers
    headers = [
        "Store", "Employé", "CIN", "Fonction", "Sal. Base", 
        "Jrs Abs.", "Déduction", "Avances", "Prêts", 
        "Ventes (Qty)", "Ventes (Rev)", 
        "Net à Payer (Est.)", "Signature"
    ]
    
    # Styles
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    center_align = Alignment(horizontal="center", vertical="center")
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    # Draw Headers
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_align
        cell.border = thin_border
        
    # Draw Rows
    row_num = 2
    for item in payroll_data:
        emp = item["employee"]
        stats = item["stats"]
        
        branch_name = emp.branch.name if emp.branch else "N/A"
        full_name = f"{emp.first_name} {emp.last_name}"
        
        row = [
            branch_name,
            full_name,
            emp.cin or "",
            emp.position,
            float(stats["salary"]),
            stats["absences"],
            float(stats["deduction"]),
            float(stats["advances"]),
            float(stats["loans"]),
            stats["sales_qty"],
            float(stats["sales_rev"]),
            float(stats["net_estimated"]),
            "" # Signature
        ]
        
        for col_num, val in enumerate(row, 1):
            cell = ws.cell(row=row_num, column=col_num, value=val)
            cell.border = thin_border
            # Format Currency columns
            if col_num in [5, 7, 8, 9, 11, 12]:
                cell.number_format = '#,##0.000 "DT"'
            
        row_num += 1

    # Adjust Column Widths
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 25
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['D'].width = 15
    ws.column_dimensions['L'].width = 20 # Net
    ws.column_dimensions['M'].width = 15 # Sig

    # 3. Save to Stream
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    filename = f"Payroll_{start_date}_{end_date}.xlsx"
    
    return StreamingResponse(
        output, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
