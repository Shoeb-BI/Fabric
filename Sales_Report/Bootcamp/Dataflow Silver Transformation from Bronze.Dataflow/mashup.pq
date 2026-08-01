[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "cbc7d6ad-919b-42fd-851c-ccd82426883b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_customers", ItemKind = "Table"]}[Data],
  #"Replaced value" = Table.ReplaceValue(#"Navigation 2", null, "unknown@bharatmart.com", Replacer.ReplaceValue, {"email"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value", {{"customer_id", each Text.Trim(Text.From(_)), type nullable text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Trimmed text", {{"customer_id", Int64.Type}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed duplicates", {{"birth_date", type date}}, "en-IN")
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_products_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_products = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "cbc7d6ad-919b-42fd-851c-ccd82426883b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_products", ItemKind = "Table"]}[Data],
  #"Trimmed text" = Table.TransformColumns(#"Navigation 2", {{"product_name", each Text.Trim(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Trimmed text", null, "Unknown", Replacer.ReplaceValue, {"category"})
in
  #"Replaced value";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_stores_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_stores = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "cbc7d6ad-919b-42fd-851c-ccd82426883b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_stores", ItemKind = "Table"]}[Data],
  #"Trimmed text" = Table.TransformColumns(#"Navigation 2", {{"zone", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text", {{"zone", each Text.Proper(_), type nullable text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Capitalized each word", "Zone_code", each if [zone] = "East" then "E" else if [zone] = "West" then "W" else if [zone] = "North" then "N" else if [zone] = "South" then "S" else "Unknown"),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column", {{"open_date", type date}}, "en-IN")
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_targets_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_targets = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "cbc7d6ad-919b-42fd-851c-ccd82426883b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_targets", ItemKind = "Table"]}[Data],
  #"Inserted conditional column" = Table.AddColumn(#"Navigation 2", "month_flag", each if [month] > 12 then "Invalid" else "valid"),
  #"Filtered rows" = Table.SelectRows(#"Inserted conditional column", each [month_flag] = "valid")
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_sales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "cbc7d6ad-919b-42fd-851c-ccd82426883b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_sales", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"sale_date", type date}}, "en-IN"),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([customer_id] <> null and [customer_id] <> "") and ([store_id] <> null and [store_id] <> "")),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows", {{"unit_price", Currency.Type}, {"amount", Currency.Type}})
in
  #"Changed column type 1";
shared dim_customers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_customers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_products_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_products", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_stores_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_stores", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_targets_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_targets", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_sales_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "fact_sales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
