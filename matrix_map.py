from coding_matrix import SPECIAL_TYPE_MAPPINGS, Coding_Matrix
SPECIAL_CODES = {'0K35', '024P', '067N'}
# Location codes for 67N
LOCATION_CODES_67N = {
   '029N','030N','031N','032N','033N','034N','039N','042N','045N','046N','048N',
   '055N','060N','0827','0839','0847','0850','0851','0857','0881','0882','0884',
   '0885','0886','0888','0889','0903','0951','0W17'
}
# Location codes for 97H
LOCATION_CODES_97H = {'029G', '030G', '031G'}

class MatrixMapper:
   def determine_profit_center(self, row):
       # ------- Existing conditions -------
       consignor = row.get("Consignor")
       consignee = row.get("Consignee")
       carrier = row.get("Carrier Name")
       consignee_code = row.get("Consignee Code")
       consignor_code = row.get("Consignor Code")
       origin_address = row.get("Origin Address")
       consignor_type = row.get("Consignor Type")
       consignee_type = row.get("Consignee Type")
       if isinstance(consignor, str) and 'averitt' in consignor.lower():
           return "0004"
       if isinstance(consignee, str) and 'coopetrajes' in consignee.lower():
           return "0896"
       if isinstance(consignor, str) and 'coopetrajes' in consignor.lower():
           return "0896"
       if consignee_code in SPECIAL_CODES:
           return consignee_code
       # ✅ New rule: If Carrier is Omnitrans, charge to DESTINATION
       if isinstance(carrier, str) and 'omnitrans' in carrier.lower():
           return consignee_code
       # ✅ Condition for 67N
       if (
           consignee_code in LOCATION_CODES_67N
           and isinstance(origin_address, str)
           and origin_address.startswith("570 Math")
       ):
           return "067N"
       # ✅ Condition for 97H
       if consignee_code in LOCATION_CODES_97H:
           return "097H"
       # ------- Existing matrix logic -------
       key = (consignor_type, consignee_type)
       if key in SPECIAL_TYPE_MAPPINGS:
           return SPECIAL_TYPE_MAPPINGS[key]
       if key in Coding_Matrix:
           direction = Coding_Matrix[key]
           if direction == "ORIGIN":
               return consignor_code
           elif direction == "DESTINATION":
               return consignee_code
       return "UNKNOWN"

