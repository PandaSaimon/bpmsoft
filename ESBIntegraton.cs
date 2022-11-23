namespace Terrasoft.Configuration.ATESBRestClientService
{
	using System;
	using System.IO;
	using System.Web;
	using System.Linq;
	using System.Text;
	using System.Xml;
	using System.Data;
	using System.Collections;
	using System.Collections.Generic;
	using System.ServiceModel;
	using System.ServiceModel.Web;
	using System.ServiceModel.Activation;
	using Terrasoft.Web.Common;
	using Terrasoft.Common;
	using Terrasoft.Core;
	using Terrasoft.Core.DB;
	using Terrasoft.Core.Entities;
	using Quartz;
	using Quartz.Impl.Triggers;
	using Terrasoft.Core.Scheduler;
	using global::Common.Logging;
 
	public static class ATESBLoggerUtilities {
		private static readonly ILog _log = LogManager.GetLogger("Terrasoft.Configuration.ATESBRestClientService");
		 
		public static void Info(string message) {
			_log.Info(message);
		}
		 
		public static void Debug(string message) {
			_log.Debug(message);
		}
		 
		public static void Error(string message) {
			_log.Error(message);
		}
	}

 
	[ServiceContract]
	[AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Required)]
	public class ATESBRestClientService
	{
		[OperationContract]
		[WebInvoke(Method = "POST", BodyStyle = WebMessageBodyStyle.Bare)]
		public bool GetMessage(Stream input)
		{
			UserConnection UserConnection = (HttpContext.Current.Application["AppConnection"] as AppConnection).SystemUserConnection;
			string AuthToken = Terrasoft.Core.Configuration.SysSettings.GetValue(UserConnection, "ATESBAuthToken").ToString();
			string[] headerValues;
			var receivedToken = string.Empty;
			var Request = HttpContext.Current.Request;
			headerValues = Request.Headers.GetValues("Authorization");
			if (headerValues != null)
			{
				receivedToken = headerValues[0];
			}
			if(AuthToken != receivedToken) {
				ATESBLoggerUtilities.Error("Access is denied. Invalid token value.");
				return false;
			}
			string Body = "";
			string ClassId = "";
			string MessageId = "";
			if (Request.QueryString["ClassId"] != null)
				ClassId = Request.QueryString["ClassId"];
			if (Request.QueryString["MessageId"] != null)
				MessageId = Request.QueryString["MessageId"];
			using (StreamReader inputStream = new StreamReader(input))
			{
				Body = inputStream.ReadToEnd();
			}
			string schedulerJobName = "ProcessESBMessage";
			string schedulerJobGroupName = "ProcessESBMessage " + MessageId != "" ? MessageId : Guid.NewGuid().ToString();
			string jobProcessName = "ATProcessESBMessageProcess";
			IDictionary<string, object> parameters = new Dictionary<string, object>();
			parameters["ClassId"] = ClassId;
			parameters["Message"] = Body;
			parameters["MessageId"] = MessageId != "" ? MessageId : "No id";
			AppScheduler.ScheduleImmediateProcessJob(schedulerJobName, schedulerJobGroupName, jobProcessName, UserConnection.Workspace.Name, "esb", parameters);
			return true;
		}
	}
	/******************************************
	************Class ESB message*************
	******************************************/
	public class ESBMessage
	{
		
		private const string SynchronizationKeyField = "КлючСинхронизации";
		public UserConnection UserConnection { get; set; }
		public string ClassId { get; set; }
		public string Body { get; set; }
		public string MessageId { get; set; }
		private Dictionary<string, string> Message { get; set; }
		private Dictionary<string, List<string>> FieldsMap { get; set; }
		private string StartSchemaName { get; set; }
		private string SchemaName { get; set; }
		private string Owner { get; set; }
		private bool isDeleted { get; set; }
		
		/******************************************
		*****************Construct*****************
		******************************************/
		public ESBMessage(UserConnection userConnection, string classId, string body, string messageId) {
			this.UserConnection = userConnection;
			this.ClassId = classId;
			this.Body = body;
			this.MessageId = messageId;
			this.Message = new Dictionary<string, string>();
			this.FieldsMap = new Dictionary<string, List<string>>();
			this.SchemaName = String.Empty;
			this.isDeleted = false;
		}
		
		/******************************************
		***************Main function***************
		******************************************/
		public void ProcessMessage() {
			try {
				ConvertFromXml();
				var sel = new Select(UserConnection)
					.Column("ATBPMObject")
				.From("ATESBIntegrationObject")
				.Where("ATESBClass").IsEqual(Column.Parameter(ClassId))
				as Select;
				SchemaName = sel.ExecuteScalar<string>();
				if(String.IsNullOrEmpty(SchemaName)) {
					ATESBLoggerUtilities.Error("SchemaName not found. ClassId: " + ClassId + ". MessageId: " + MessageId + "\r\n\r\nMessage: " + Body+ "\r\n\r\n\r\n\r\n");
					return;
				}
				StartSchemaName = SchemaName;
				if(SchemaName == "Account"){
					if(!Convert.ToBoolean(Message["ЭтоГруппа"])) {
						SchemaName = "NrbLegalEntity";
						if(Message["Родитель"].ToLower() == "ESU_2fbb3d4a-12bb-11e7-9f07-00155d902b00".ToLower()) {
							Message["Родитель"] = "ESU_ec9a587c-9d94-11e5-bd4a-00155d902b00";
						} else if(Message["Родитель"].ToLower() == "ESU_bcfea247-8496-11dd-bc02-000423d2dd69".ToLower()) {
							Message["Родитель"] = "ESU_b1abbc34-79ce-11e2-a001-005056c00008";
						} else if(Message["Родитель"].ToLower() == "ESU_3a63e219-fcbc-11e6-a5d0-00155d902b00".ToLower()) {
							Message["Родитель"] = "ESU_f0e03645-e2fa-11e6-8366-00155d902b00";
						} else if(Message["Родитель"].ToLower() == "ESU_0f345227-e6ba-11e6-8366-00155d902b00".ToLower()) {
							Message["Родитель"] = "ESU_f0e03645-e2fa-11e6-8366-00155d902b00";
						}
					} else if(Message["КлючСинхронизации"].ToLower() == "ESU_2fbb3d4a-12bb-11e7-9f07-00155d902b00".ToLower()
					|| Message["КлючСинхронизации"].ToLower() == "ESU_bcfea247-8496-11dd-bc02-000423d2dd69".ToLower() 
					|| Message["КлючСинхронизации"].ToLower() == "ESU_3a63e219-fcbc-11e6-a5d0-00155d902b00".ToLower()
					|| Message["КлючСинхронизации"].ToLower() == "ESU_0f345227-e6ba-11e6-8366-00155d902b00".ToLower()) {
						return;
					}
				} else if(SchemaName == "CommunicationType") {
					if(String.IsNullOrEmpty(Message["Наименование"])) {
						return;
					}
					if(Message["Тип"] == "Адрес") {
						SchemaName = "AddressType";
					}
				}
				
				GetFieldsMap();
				if(FieldsMap.Count == 0) {
					ATESBLoggerUtilities.Error("Fields map not found. ClassId: " + ClassId + ". MessageId: " + MessageId + "\r\n\r\nMessage: " + Body+ "\r\n\r\n\r\n\r\n");
					return;
				}
				if(SchemaName == "NrbLegalEntity" && StartSchemaName != "NrbLegalEntity") {
					if(Message["Родитель"].IsNullOrEmpty()) {
						return;
					}
				} else if(SchemaName == "Contact") {
					sel = new Select(UserConnection)
						.Column("Id")
						.From("NrbLegalEntity")
						.Where("NrbSynchronizationKey").IsEqual(Column.Parameter(Message["Владелец"]))
					as Select;
					Guid NrbLegalEntity = sel.ExecuteScalar<Guid>();
					if(NrbLegalEntity == Guid.Empty) {
						return;
					}
				}
				isDeleted = Convert.ToBoolean(Message["ПометкаУдаления"]);
				Guid RecordId = SaveRecord();
				if(RecordId != Guid.Empty) {
					SaveAdditionalRecords(RecordId);
				}
			}
			catch(Exception e) {
				ATESBLoggerUtilities.Error("Error. ClassId: " + ClassId + ". MessageId: " + MessageId + "\r\n\r\nMessage: " + Body + "\r\n\r\nError description: " + e.ToString()+ "\r\n\r\n\r\n\r\n");
			}
		}
		
		///////////////////////////////////////////////////////////////
		////////////////////////Private methods////////////////////////
		///////////////////////////////////////////////////////////////
		
		/******************************************
		*************Get struct message************
		******************************************/
		private void ConvertFromXml() {
			XmlDocument Xml = new XmlDocument();
			Xml.LoadXml(Body);
			var Document = Xml.DocumentElement;
			foreach (XmlNode Node in Document.ChildNodes) {
				Message.Add(Node.Name, Node.InnerXml);
			}
		}
		
		/******************************************
		***********Get struct sub message**********
		******************************************/
		private List<Dictionary<string, string>> ConvertRowsFromXml(string rows) {
			Dictionary<string, string> tmp = new Dictionary<string, string>();
			List<Dictionary<string, string>> result = new List<Dictionary<string, string>>();
			XmlDocument Xml = new XmlDocument();
			Xml.LoadXml(rows);
			var Document = Xml.DocumentElement;
			foreach (XmlNode Row in Document.ChildNodes) {
				foreach (XmlNode Node in Row.ChildNodes) {
					tmp.Add(Node.Name, Node.InnerXml);
				}
				result.Add(tmp);
				tmp = new Dictionary<string, string>();
			}
			return result;
		}
		
		/******************************************
		***********Get struct fields map***********
		******************************************/
		private void GetFieldsMap() {
			var sel = new Select(UserConnection)
				.Column("ATESBField")
				.Column("ATBPMField")
			.From("ATESBIntegrationField")
			.Where("ATBPMEntity").IsEqual(Column.Parameter(SchemaName))
			as Select;
			using (DBExecutor dbExecutor = UserConnection.EnsureDBConnection())
			{
				using (IDataReader reader = sel.ExecuteReader(dbExecutor))
				{
					while (reader.Read())
					{
						string ATESBField = reader.GetColumnValue("ATESBField").ToString();
						string ATBPMField = reader.GetColumnValue("ATBPMField").ToString();
						if(FieldsMap.ContainsKey(ATESBField)) {
							FieldsMap[ATESBField].Add(ATBPMField);
						} else {
							FieldsMap.Add(ATESBField, new List<string>());
							FieldsMap[ATESBField].Add(ATBPMField);
						}
						string ReferenceSchemaName = IsLookupType(ATBPMField);
						if(!String.IsNullOrEmpty(ReferenceSchemaName)) {
							if(Message.ContainsKey(ATESBField)) {
								string SynchronizationKeyField = "NrbSynchronizationKey";
								string value = Message[ATESBField];
								if(!String.IsNullOrEmpty(value)) {
									if(ReferenceSchemaName == "Contact" && ATBPMField.Contains("Owner")) {
										SynchronizationKeyField = "ATLoginAD";
										value = value.Substring(value.LastIndexOf("\\") + 1);
										if(String.IsNullOrEmpty(value)) {
											value = "esb";
										}
										Owner = value;
									} else if(ReferenceSchemaName == "ATCommunicationType") {
										SynchronizationKeyField = "AT1CName";
									} else if(ReferenceSchemaName == "ATAddressType") {
										SynchronizationKeyField = "Name";
									}
									sel = new Select(UserConnection)
										.Column("Id")
									.From(ReferenceSchemaName)
									.Where(SynchronizationKeyField).IsEqual(Column.Parameter(value))
									as Select;
									Message[ATESBField] = sel.ExecuteScalar<string>();
								}
							}
						}
					}
				}
			}
		}
		
		/******************************************
		***********Check is lookup column**********
		***********returns*************************
		***********empty string********************
		***********or ReferenceSchemaName**********
		******************************************/
		private string IsLookupType(string Path) {
			string result = String.Empty;
			var Manager = UserConnection.EntitySchemaManager.GetInstanceByName(SchemaName);
			var Column = Manager.Columns.GetByColumnValueName(Path);
			if(Column.IsLookupType) {
				result = Column.ReferenceSchema.Name;
			}
			return result;
		}
		
		/******************************************
		****************Save record****************
		******************************************/
		private Guid SaveRecord() {
			string SynchronizationKeyColumn = FieldsMap[SynchronizationKeyField][0];
			Guid PrimaryId = Guid.Empty;
			var esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, SchemaName);
			esq.AddAllSchemaColumns();
			esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, SynchronizationKeyColumn, Message[SynchronizationKeyField]));
			if(SchemaName == "Account") {
				esq.Filters.LogicalOperation = LogicalOperationStrict.Or;
				esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "Nrb1CCode", Message["Код"]));
			}
			if(SchemaName == "NrbLegalEntity") {
				esq.Filters.LogicalOperation = LogicalOperationStrict.Or;
				esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "NrbTIN", Message["ИНН"]));
			}
			EntityCollection CheckCollection = esq.GetEntityCollection(UserConnection);
			if(CheckCollection.Count > 0) {
				var entity = CheckCollection[0];
				PrimaryId = entity.GetTypedColumnValue<Guid>("Id");
				foreach(var Pair in FieldsMap) {
					if(Message.ContainsKey(Pair.Key)) {
						if(!String.IsNullOrEmpty(Message[Pair.Key])) {
							foreach(string field in Pair.Value) {
								if(entity.GetTypedColumnValue<string>(field).ToLower() != Message[Pair.Key].ToLower()) {
									entity.SetColumnValue(field, Message[Pair.Key]);
								}
							}
						}
					}
				}
				entity = SaveAdditionalLogic(entity, SchemaName);
				entity.Save();
			} else {
				if(!isDeleted) {
					bool isNew = true;
					if(SchemaName == "NrbLegalEntity") {
						isNew = false;
						esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, SchemaName);
						esq.AddAllSchemaColumns();
						esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "NrbTIN", Message["ИНН"]));
						esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "NrbRRC", Message["КПП"]));
						esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "RecordInactive", Message["ПометкаУдаления"]));
						CheckCollection = esq.GetEntityCollection(UserConnection);
						if(CheckCollection.Count > 0) {
							var entity = CheckCollection[0];
							PrimaryId = entity.GetTypedColumnValue<Guid>("Id");
							foreach(var Pair in FieldsMap) {
								if(Message.ContainsKey(Pair.Key)) {
									if(!String.IsNullOrEmpty(Message[Pair.Key])) {
										foreach(string field in Pair.Value) {
											if(entity.GetTypedColumnValue<string>(field).ToLower() != Message[Pair.Key].ToLower()) {
												entity.SetColumnValue(field, Message[Pair.Key]);
											}
										}
									}
								}
							}
							entity = SaveAdditionalLogic(entity, SchemaName);
							entity.Save();
						} else {
							isNew = true;
						}
					}
					if(isNew) {
						EntitySchema schema = UserConnection.EntitySchemaManager.GetInstanceByName(SchemaName);
						Entity newEntity = schema.CreateEntity(UserConnection);
						newEntity.SetDefColumnValues();
						foreach(var Pair in FieldsMap) {
							if(Message.ContainsKey(Pair.Key)) {
								if(!String.IsNullOrEmpty(Message[Pair.Key])) {
									foreach(string field in Pair.Value) {
										newEntity.SetColumnValue(field, Message[Pair.Key]);
									}
								}
							}
						}
						newEntity = SaveAdditionalLogic(newEntity, SchemaName);
						newEntity.Save();
						PrimaryId = newEntity.GetTypedColumnValue<Guid>("Id");
					}
				}
			}
			return PrimaryId;
		}
		
		/******************************************
		***********Save additional logic***********
		******************************************/
		private Entity SaveAdditionalLogic(Entity entity, string SchemaName) {
			if(SchemaName == "Contact") {
				entity.SetColumnValue("TypeId", new Guid("1603E141-0FCE-45BF-8EC3-3239FB684270"));
				entity.SetColumnValue("NrbIsMDMApproved", true);
			} else 
			if(SchemaName == "Account") {
				if(Owner == "d.razymovskaya" || Owner == "a.goncharov" || Owner == "a.morozov2" || Owner == "o.atamas") {
					entity.SetColumnValue("TypeId", new Guid("643D7034-8DF0-45E0-8EA2-87461B4B646E"));
				} else {
					entity.SetColumnValue("TypeId", new Guid("f2c0ce97-53e6-df11-971b-001d60e938c6"));
				}
				if(entity.GetTypedColumnValue<Guid>("CityId") != Guid.Empty) {
					var sel = new Select(UserConnection)
						.Column("NrbDistrictId")
						.From("City")
						.Where("Id").IsEqual(Column.Parameter(entity.GetTypedColumnValue<Guid>("CityId")))
					as Select;
					Guid NrbDistrict = sel.ExecuteScalar<Guid>();
					if(NrbDistrict != Guid.Empty) {
						entity.SetColumnValue("NrbDistrictId", NrbDistrict);
					}
				}
				entity.SetColumnValue("NrbIsMDMApproved", true);
			} else 
			if(SchemaName == "NrbLegalEntity") {
				if(StartSchemaName == "NrbLegalEntity") {
					entity.SetColumnValue("NrbAccountId", new Guid("57a629b0-e11c-4102-8f0b-a4672baa9e26"));
					Message.Add("Родитель", "BPM_57a629b0-e11c-4102-8f0b-a4672baa9e26");
				}
				entity.SetColumnValue("NrbIsMDMApproved", true);
			} else 
			if(SchemaName == "CommunicationType") {
				entity.SetColumnValue("UseforAccounts", false);
				entity.SetColumnValue("UseforContacts", false);
				entity.SetColumnValue("NrbUseforLegalEntities", false);
			} else 
			if(SchemaName == "AddressType") {
				entity.SetColumnValue("ForContact", false);
				entity.SetColumnValue("ForAccount", false);
				entity.SetColumnValue("NrbForLegalEntity", false);
			} else 
			if(SchemaName == "NrbBankLookup") {
				if(String.IsNullOrEmpty(entity.GetTypedColumnValue<string>("Name")))
					entity.SetColumnValue("Name", "Не заполнено");
			}
			return entity;
		}
		
		/******************************************
		**********Save additional records**********
		******************************************/
		private void SaveAdditionalRecords(Guid PrimaryId) {
			switch (SchemaName) {
				case "NrbLegalEntity": {
					SaveNrbLegalEntityAdditionalRecords(PrimaryId);
					break;
				}
				
				case "Contact": {
					SaveContactAdditionalRecords(PrimaryId);
					break;
				}
				
				case "Account": {
					SaveAccountAdditionalRecords(PrimaryId);
					break;
				}
				default: {
					break;
				}
			}
		}
		
		/******************************************
		******Save contact additional records******
		******************************************/
		private void SaveContactAdditionalRecords(Guid PrimaryId) {
			SaveContactInfo("Contact", "Contact", PrimaryId, PrimaryId);
			if(Message.ContainsKey("Владелец") && !String.IsNullOrEmpty(Message["Владелец"])) {
				var sel = new Select(UserConnection)
					.Column("Id")
					.From("NrbLegalEntity")
					.Where("NrbSynchronizationKey").IsEqual(Column.Parameter(Message["Владелец"]))
				as Select;
				Guid NrbLegalEntity = sel.ExecuteScalar<Guid>();
				if(NrbLegalEntity != Guid.Empty) {
					var esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "NrbLegalEntityContact");
					esq.AddAllSchemaColumns();
					esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "NrbContact", PrimaryId));
					EntityCollection Contacts = esq.GetEntityCollection(UserConnection);
					if(Contacts.Count > 0) {
						Contacts[0].SetColumnValue("NrbLegalEntityId", NrbLegalEntity);
						Contacts[0].Save();
					} else {
						EntitySchema schema = UserConnection.EntitySchemaManager.GetInstanceByName("NrbLegalEntityContact");
						Entity entity = schema.CreateEntity(UserConnection);
						entity.SetDefColumnValues();
						entity.SetColumnValue("NrbLegalEntityId", NrbLegalEntity);
						entity.SetColumnValue("NrbContactId", PrimaryId);
						entity.Save();
					}
					sel = new Select(UserConnection)
						.Column("NrbAccountId")
						.From("NrbLegalEntity")
						.Where("Id").IsEqual(Column.Parameter(NrbLegalEntity))
					as Select;
					Guid Account = sel.ExecuteScalar<Guid>();
					if(Account != Guid.Empty) {
						esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "Contact");
						esq.AddAllSchemaColumns();
						Entity contact = esq.GetEntity(UserConnection, PrimaryId);
						contact.SetColumnValue("AccountId", Account);
						contact.Save();
					}
				}
			}
		}
		
		/******************************************
		******Save account additional records******
		******************************************/
		private void SaveAccountAdditionalRecords(Guid PrimaryId) {
			if(!String.IsNullOrEmpty(Message["ТипыЦенГруппКонтрагентов"])) {
				List<Dictionary<string, string>> data = ConvertRowsFromXml("<classData>"+Message["ТипыЦенГруппКонтрагентов"]+"</classData>");
				
				var sel = new Select(UserConnection)
					.Column(Func.Count(Column.Asterisk()))
					.From("NrbAccountPartnerProductCategory")
					.Where("NrbAccountId").IsEqual(Column.Parameter(PrimaryId))
				as Select;
				bool isAddAll = sel.ExecuteScalar<int>() == 0;
				string NrbSynchronizationKey = Message["КлючСинхронизации"];
				
				foreach(Dictionary<string, string> row in data) {
					
					Guid NrbProductCategoryId = Guid.Empty;
					sel = new Select(UserConnection)
						.Column("Id")
						.From("NrbProductCategoryLookup")
						.Where("NrbSynchronizationKey").IsEqual(Column.Parameter(row["ПланТиповЦен"]))
					as Select; 
					string NrbProductCategory = sel.ExecuteScalar<string>();
					if(!String.IsNullOrEmpty(NrbProductCategory)) {
						NrbProductCategoryId = new Guid(NrbProductCategory);
					}
					
					Guid NrbPriceTypeId = Guid.Empty;
					sel = new Select(UserConnection)
						.Column("Id")
						.From("NrbPriceTypeLookup")
						.Where("NrbSynchronizationKey").IsEqual(Column.Parameter(row["ТипЦен"]))
					as Select; 
					string NrbPriceType = sel.ExecuteScalar<string>();
					if(!String.IsNullOrEmpty(NrbPriceType)) {
						NrbPriceTypeId = new Guid(NrbPriceType);
					}
					
					/*Guid OwnerId = Guid.Empty;
					sel = new Select(UserConnection)
						.Column("Id")
						.From("Contact")
						.Where("NrbLogin").IsEqual(Column.Parameter(row["Ответственный"]))
					as Select; 
					string Owner = sel.ExecuteScalar<string>();
					if(!String.IsNullOrEmpty(Owner)) {
						OwnerId = new Guid(Owner);
					}*/
					if(NrbProductCategoryId != Guid.Empty && ((!String.IsNullOrEmpty(row["ТипЦен"]) 
					&& NrbPriceTypeId != Guid.Empty) || String.IsNullOrEmpty(row["ТипЦен"])))
					{
						Guid Id = Guid.Empty;
						sel = new Select(UserConnection)
							.Top(1)
							.Column("Id")
							.From("NrbAccountPartnerProductCategory")
							.Where("NrbAccountId").IsEqual(Column.Parameter(PrimaryId))
							.And("NrbDataFromDate").IsEqual(Column.Parameter(Convert.ToDateTime(row["Период"])))
							.And("NrbProductCategoryId").IsEqual(Column.Parameter(NrbProductCategoryId))
						as Select;
						Id = sel.ExecuteScalar<Guid>();
						if(isAddAll || Id == Guid.Empty) {
							EntitySchema schema = UserConnection.EntitySchemaManager.GetInstanceByName("NrbAccountPartnerProductCategory");
							Entity entity = schema.CreateEntity(UserConnection);
							entity.SetDefColumnValues();
							entity.SetColumnValue("NrbDataFromDate", Convert.ToDateTime(row["Период"]));
							entity.SetColumnValue("NrbProductCategoryId", NrbProductCategoryId);
							if(NrbPriceTypeId != Guid.Empty)
								entity.SetColumnValue("NrbPriceTypeId", NrbPriceTypeId);
							/*if(OwnerId != Guid.Empty)
								entity.SetColumnValue("NrbOwnerId", OwnerId);*/
							entity.SetColumnValue("NrbSynchronizationKey", NrbSynchronizationKey);
							entity.SetColumnValue("NrbAccountId", PrimaryId);
							entity.Save();
						} else {
							var esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "NrbAccountPartnerProductCategory");
							esq.AddAllSchemaColumns();
							Entity partnerProductCategory = esq.GetEntity(UserConnection, Id);
							if(NrbPriceTypeId != Guid.Empty)
								partnerProductCategory.SetColumnValue("NrbPriceTypeId", NrbPriceTypeId);
							partnerProductCategory.Save();
						}
					}
				}
			}
		}
		
		/*****************************************
		**Save NrbLegalEntity additional records**
		******************************************/
		private void SaveNrbLegalEntityAdditionalRecords(Guid PrimaryId) {
			
			var sel = new Select(UserConnection)
				.Column("NrbAccountId")
				.From("NrbLegalEntity")
				.Where("Id").IsEqual(Column.Parameter(PrimaryId))
			as Select;
			string Account = sel.ExecuteScalar<string>();
			Guid AccountId = Guid.Empty;
			if(!String.IsNullOrEmpty(Account)) {
				AccountId = new Guid(Account);
			}
			SaveContactInfo("NrbLegalEntity", "Account", PrimaryId, AccountId);
		}
		
		/*****************************************
		*************Save contact info************
		******************************************/
		private void SaveContactInfo(string AddressEntity, string CommunicationEntity, Guid AddressPrimaryId, Guid CommunicationPrimaryId) {
			if(!String.IsNullOrEmpty(Message["КонтактнаяИнформация"])) {
				List<Dictionary<string, string>> data = ConvertRowsFromXml("<classData>"+Message["КонтактнаяИнформация"]+"</classData>");
				var sel = new Select(UserConnection)
					.Column(Func.Count(Column.Asterisk()))
					.From(AddressEntity+"Address")
					.Where(AddressEntity+"Id").IsEqual(Column.Parameter(AddressPrimaryId))
				as Select;
				bool isAddAllAddress = sel.ExecuteScalar<int>() == 0;
				
				sel = new Select(UserConnection)
					.Column(Func.Count(Column.Asterisk()))
					.From(CommunicationEntity+"Communication")
					.Where(CommunicationEntity+"Id").IsEqual(Column.Parameter(CommunicationPrimaryId))
				as Select;
				bool isAddAllCommunication = sel.ExecuteScalar<int>() == 0;
				
				List<Dictionary<string, string>> NotAdded = new List<Dictionary<string, string>>();
				Dictionary<string, string> tmp = new Dictionary<string, string>();
				List<string> Added = new List<string>();
				
				foreach(Dictionary<string, string> row in data) {
					if(row["Представление"].IsNullOrEmpty())
						continue;
					string PrimaryEntity = CommunicationEntity;
					string KindTable = "CommunicationType";
					string ValueColumn = "Number";
					string TableToInsert = "Communication";
					if(row["Тип"] == "Адрес") {
						PrimaryEntity = AddressEntity;
						KindTable = "AddressType";
						TableToInsert = "Address";
						ValueColumn = "Address";
					}
					
					Guid TypeId = Guid.Empty;
					string type = row["Тип"];
					switch(type) {
						case "АдресЭлектроннойПочты": {
							type = "E-Mail";
							break;
						}
						case "ВебСтраница": {
							type = "Веб-страница";
							break;
						}
					}
					sel = new Select(UserConnection)
						.Column("Id")
						.From("AT"+TableToInsert+"Type")
						.Where("Name").IsEqual(Column.Parameter(type))
					as Select;
					string Type = sel.ExecuteScalar<string>();
					if(!String.IsNullOrEmpty(Type)) {
						TypeId = new Guid(Type);
					}
					
					sel = new Select(UserConnection)
						.Column("NrbSynchronizationKey")
						.From(PrimaryEntity)
						.Where("Id").IsEqual(Column.Parameter(TableToInsert == "Address" ? AddressPrimaryId : CommunicationPrimaryId))
					as Select;
					string NrbSynchronizationKey = sel.ExecuteScalar<string>();
					
					Guid KindId = Guid.Empty;
					sel = new Select(UserConnection)
						.Column("Id")
						.From(KindTable)
						.Where("NrbSynchronizationKey").IsEqual(Column.Parameter(row["Вид"]))
					as Select; 
					string Kind = sel.ExecuteScalar<string>();
					if(String.IsNullOrEmpty(Kind)) {
						continue;
					}
					KindId = new Guid(Kind);
					
					string value = row["Представление"];
					bool isPrimary = Convert.ToBoolean(row["Основной"]);
					if(TableToInsert != "Communication" || CommunicationPrimaryId != Guid.Empty) {
						if((TableToInsert == "Address" && isAddAllAddress) || (TableToInsert == "Communication" && isAddAllCommunication)) {
							EntitySchema schema = UserConnection.EntitySchemaManager.GetInstanceByName(PrimaryEntity+TableToInsert);
							Entity entity = schema.CreateEntity(UserConnection);
							entity.SetDefColumnValues();
							entity.SetColumnValue("NrbSynchronizationKey", NrbSynchronizationKey);
							entity.SetColumnValue(ValueColumn, value);
							if(KindId != Guid.Empty)
								entity.SetColumnValue(KindTable+"Id", KindId);
							if(TypeId != Guid.Empty)
								entity.SetColumnValue("ATTypeId", TypeId);
							if(PrimaryEntity != "Contact" || TableToInsert != "Communication")
								entity.SetColumnValue("Primary", isPrimary);
							entity.SetColumnValue(PrimaryEntity+"Id", TableToInsert == "Address" ? AddressPrimaryId : CommunicationPrimaryId);
							if(PrimaryEntity == "Account" && TableToInsert == "Communication") {
								entity.SetColumnValue("ATLegalEntityId", AddressPrimaryId);
							}
							entity.SetColumnValue("RecordInactive", false);
							entity.Save();
							Added.Add(entity.GetTypedColumnValue<string>("Id"));
						} else {
							sel = new Select(UserConnection)
								.Column("Id")
								.From(PrimaryEntity+TableToInsert)
								.Where(KindTable+"Id").IsEqual(Column.Parameter(KindId))
								.And("ATTypeId").IsEqual(Column.Parameter(TypeId))
								.And(ValueColumn).IsEqual(Column.Parameter(value))
								.And(PrimaryEntity+"Id").IsEqual(Column.Parameter(TableToInsert == "Address" ? AddressPrimaryId : CommunicationPrimaryId))
							as Select;
							string infoId = sel.ExecuteScalar<string>();
							Guid InfoId = Guid.Empty;
							if(!String.IsNullOrEmpty(infoId)) {
								InfoId = new Guid(infoId);
							}
						
							if(InfoId == Guid.Empty) {
								tmp.Add("TypeId", TypeId.ToString());
								tmp.Add("KindId", KindId.ToString());
								tmp.Add("KindTable", KindTable);
								tmp.Add("TableToInsert", TableToInsert);
								tmp.Add("ValueColumn", ValueColumn);
								tmp.Add("value", value);
								tmp.Add("isPrimary", isPrimary.ToString());
								tmp.Add("PrimaryEntity", PrimaryEntity);
								tmp.Add("NrbSynchronizationKey", NrbSynchronizationKey);
								NotAdded.Add(tmp);
								tmp = new Dictionary<string, string>();
							} else {
								Added.Add(InfoId.ToString());
								var esq1 = new EntitySchemaQuery(UserConnection.EntitySchemaManager, PrimaryEntity+TableToInsert);
								esq1.AddAllSchemaColumns();
								Entity Info = esq1.GetEntity(UserConnection, InfoId);
								if(Info.GetTypedColumnValue<string>("NrbSynchronizationKey") != Message[SynchronizationKeyField] 
								|| Info.GetTypedColumnValue<bool>("RecordInactive") || (TableToInsert == "Communication" 
								&& PrimaryEntity == "Account" && Info.GetTypedColumnValue<Guid>("ATLegalEntityId") != AddressPrimaryId)) {
									if(PrimaryEntity == "Account" && TableToInsert == "Communication") {
										Info.SetColumnValue("ATLegalEntityId", AddressPrimaryId);
									}
									Info.SetColumnValue("NrbSynchronizationKey", NrbSynchronizationKey);
									Info.SetColumnValue("RecordInactive", false);
									Info.Save();
								}
							}
						}
					}
				}
				if(NotAdded.Count > 0) {
					var esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, CommunicationEntity+"Communication");
					esq.AddAllSchemaColumns();
					if(Added.Count > 0) {
						esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.NotEqual, "Id", Added.ToArray()));
					}
					esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, CommunicationEntity, CommunicationPrimaryId));
					if(CommunicationEntity == "Account") {
						esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "ATLegalEntity", AddressPrimaryId));
					}
					esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.NotEqual, "NrbSynchronizationKey", ""));
					EntityCollection Infos = esq.GetEntityCollection(UserConnection);
					foreach(var Info in Infos) {
						string CommunicationType = Info.GetTypedColumnValue<string>("CommunicationTypeId");
						string ATType = Info.GetTypedColumnValue<string>("ATTypeId");
						bool isUpdate = false;
						for(int i = 0; i < NotAdded.Count; i++) {
							if(NotAdded[i]["KindId"] == CommunicationType && NotAdded[i]["TypeId"] == ATType && NotAdded[i]["TableToInsert"] == "Communication") {
								Info.SetColumnValue("Number", NotAdded[i]["value"]);
								isUpdate = true;
								NotAdded.RemoveAt(i);
								break;
							}
						}
						if(!isUpdate) {
							Info.SetColumnValue("RecordInactive", true);
						} else {
							Info.SetColumnValue("RecordInactive", false);
						}
						Info.Save();
					}
					
					esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, AddressEntity+"Address");
					esq.AddAllSchemaColumns();
					if(Added.Count > 0) {
						esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.NotEqual, "Id", Added.ToArray()));
					}
					esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, AddressEntity, AddressPrimaryId));
					esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.NotEqual, "NrbSynchronizationKey", ""));
					Infos = esq.GetEntityCollection(UserConnection);
					foreach(var Info in Infos) {
						string AddressType = Info.GetTypedColumnValue<string>("AddressTypeId");
						string ATType = Info.GetTypedColumnValue<string>("ATTypeId");
						bool isUpdate = false;
						for(int i = 0; i < NotAdded.Count; i++) {
							if(NotAdded[i]["KindId"] == AddressType && NotAdded[i]["TypeId"] == ATType && NotAdded[i]["TableToInsert"] == "Address") {
								Info.SetColumnValue("Address", NotAdded[i]["value"]);
								isUpdate = true;
								NotAdded.RemoveAt(i);
								break;
							}
						}
						if(!isUpdate) {
							Info.SetColumnValue("RecordInactive", true);
						} else {
							Info.SetColumnValue("RecordInactive", false);
						}
						Info.Save();
					}
					
					foreach(var element in NotAdded) {
						if(element["TableToInsert"] != "Communication" || CommunicationPrimaryId != Guid.Empty) {
							EntitySchema schema = UserConnection.EntitySchemaManager.GetInstanceByName(element["PrimaryEntity"]+element["TableToInsert"]);
							Entity entity = schema.CreateEntity(UserConnection);
							entity.SetDefColumnValues();
							entity.SetColumnValue("NrbSynchronizationKey", element["NrbSynchronizationKey"]);
							entity.SetColumnValue(element["ValueColumn"], element["value"]);
							if(new Guid(element["KindId"]) != Guid.Empty)
								entity.SetColumnValue(element["KindTable"]+"Id", element["KindId"]);
							if(new Guid(element["TypeId"]) != Guid.Empty)
								entity.SetColumnValue("ATTypeId", element["TypeId"]);
							if(element["PrimaryEntity"] != "Contact" || element["TableToInsert"] != "Communication")
								entity.SetColumnValue("Primary", element["isPrimary"]);
							entity.SetColumnValue(element["PrimaryEntity"]+"Id", element["TableToInsert"] == "Address" ? AddressPrimaryId : CommunicationPrimaryId);
							if(element["PrimaryEntity"] == "Account" && element["TableToInsert"] == "Communication") {
								entity.SetColumnValue("ATLegalEntityId", AddressPrimaryId);
							}
							entity.SetColumnValue("RecordInactive", false);
							entity.Save();
						}
					}
				}
			}
		}
	}
}
