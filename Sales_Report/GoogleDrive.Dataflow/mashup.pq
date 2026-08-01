[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "Query_DataDestinationTransform"]]]}]
shared Query = let
  Source = Csv.Document(Web.Contents("https://drive.google.com/uc?export=download&id=10TwQm_GaOk4NmGoEAr6wHS7XFX8nzz2F"), [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"sale_id", Int64.Type}, {"customer_id", Int64.Type}, {"product_id", Int64.Type}, {"store_id", Int64.Type}, {"sale_date", type text}, {"quantity", Int64.Type}, {"unit_price", type number}, {"amount", type number}})
in
  #"Changed column type";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "d1847af3-63f2-4cdb-ac61-879503d263b2"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1042578f-a917-48cd-957a-35c6985b3258"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "Google Drive"]}[Content],
  FileNavigation = Navigation_4{[Name = "FactSale"]}?[Content]?
in
  FileNavigation;
shared Query_DataDestinationTransform = Csv.OutputConfiguration([Encoding = 65001, Delimiter = ","]);
