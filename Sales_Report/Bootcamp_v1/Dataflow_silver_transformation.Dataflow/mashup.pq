[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1042578f-a917-48cd-957a-35c6985b3258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_customers", ItemKind = "Table"]}[Data],
  #"Replaced value" = Table.ReplaceValue(#"Navigation 2", null, "gen2@bootcamp.com", Replacer.ReplaceValue, {"email"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value", {{"customer_id", each Text.Trim(Text.From(_)), type nullable text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Trimmed text", {{"customer_id", Int64.Type}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed duplicates", {{"birth_date", type date}}, "en-IN"),
  #"Sorted rows" = Table.Sort(#"Changed column type 1", {{"customer_id", Order.Ascending}})
in
  #"Sorted rows";
shared dim_products = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1042578f-a917-48cd-957a-35c6985b3258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_products", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_stores = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1042578f-a917-48cd-957a-35c6985b3258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_stores", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_targets = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1042578f-a917-48cd-957a-35c6985b3258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_targets", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1042578f-a917-48cd-957a-35c6985b3258"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_sales", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_customers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a8d27448-9553-4ed4-8a76-fadc3aa1123e"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_customers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
