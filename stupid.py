from openpyxl import Workbook

wb = Workbook()
ws = wb.active

print(type(ws))
