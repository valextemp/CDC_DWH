
Transfer Data from CDC ASTO_ZF (testing server)

Хотел обойтись толь одной таблицей [dwh].[asto_DataValues_CT], но не учел удаленные записи и сейчас 20231013 все равно идет обращение к [dwh].[asto_Rows_CT] (для определения пользователя который удалил значение из DataValues)