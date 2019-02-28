import json
import java.io
import csv
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
 
class Table(object):
    def __init__(self):
        self._csm = None

    def __init__(self):
        self._risk = None

    def __init__(self):
        self._experience = None

    def __init__(self):
        self._contracts = None

    def __init__(self):
        self._changes_in_estimates_reflected_in_csm = None

    def __init__(self):
        self._changes_in_estimates_resulting_in_onerous_contract_losses = None

    def __init__(self):
        self._adjustments = None

    def __init__(self):
        self._insurance_finance_expenses = None

    def __init__(self):
        self._movements_in_exchange_rates = None

    def __init__(self):
        self._cashflows = None

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

    @property
    def csm(self):
        return self._csm

    @csm.setter
    def csm(self, value):
        self._csm = value

    @property
    def risk(self):
        return self._risk

    @risk.setter
    def risk(self, value):
        self._risk = value

    @property
    def experience(self):
        return self._experience

    @experience.setter
    def experience(self, value):
        self._experience = value

    @property
    def contracts(self):
        return self._contracts

    @contracts.setter
    def contracts(self, value):
        self._contracts = value

    @property
    def changes_in_estimates_reflected_in_csm(self):
        return self._changes_in_estimates_reflected_in_csm

    @changes_in_estimates_reflected_in_csm.setter
    def changes_in_estimates_reflected_in_csm(self, value):
        self._changes_in_estimates_reflected_in_csm = value

    @property
    def changes_in_estimates_resulting_in_onerous_contract_losses(self):
        return self._changes_in_estimates_resulting_in_onerous_contract_losses

    @changes_in_estimates_resulting_in_onerous_contract_losses.setter
    def changes_in_estimates_resulting_in_onerous_contract_losses(self, value):
        self._changes_in_estimates_resulting_in_onerous_contract_losses = value

    @property
    def adjustments(self):
        return self._adjustments

    @adjustments.setter
    def adjustments(self, value):
        self._adjustments = value

    @property
    def insurance_finance_expenses(self):
        return self._insurance_finance_expenses

    @insurance_finance_expenses.setter
    def insurance_finance_expenses(self, value):
        self._insurance_finance_expenses = value


    @property
    def movements_in_exchange_rates(self):
        return self._movements_in_exchange_rates

    @movements_in_exchange_rates.setter
    def movements_in_exchange_rates(self, value):
        self._movements_in_exchange_rates = value

    @property
    def cashflows(self):
        return self._cashflows

    @cashflows.setter
    def cashflows(self, value):
        self._cashflows = value

class Report(object):
    def __init__(self):
        self._epv_cashflows = None

    def __init__(self):
        self._risk_adj = None

    def __init__(self):
        self._csm = None

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

    @property
    def epv_cashflows(self):
        return self._epv_cashflows

    @epv_cashflows.setter
    def epv_cashflows(self, value):
        self._epv_cashflows = value

    @property
    def risk_adj(self):
        return self._risk_adj

    @risk_adj.setter
    def risk_adj(self, value):
        self._risk_adj = value

    @property
    def csm(self):
        return self._csm

    @csm.setter
    def csm(self, value):
        self._csm = value

class ModJSON(StreamCallback):
	def __init__(self):
		pass
  
	def process(self, inputStream, outputStream):
		intext = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
		lines = intext.split("\n")
		data = ([row.split(",") for row in lines])
		
		
		EPV_Cashflows = Table()
		Risk_Adj = Table()
		csm = Table()
		rpt = Report()
		
		for i in range(len(data)):
			if len(data[i]) >= 5:
				if  "CSM recognised for services provided" in data[i][0] :
					EPV_Cashflows.csm = checkBlank(data[i][1])
					Risk_Adj.csm = checkBlank(data[i][2])
					csm.csm = checkBlank(data[i][3])

				if  "Risk adjustment recognised for the risk expired" in data[i][0] :
					EPV_Cashflows.risk = checkBlank(data[i][1])
					Risk_Adj.risk = checkBlank(data[i][2])
					csm.risk = checkBlank(data[i][3])

				if  "Experience adjustments" in data[i][0] :
					EPV_Cashflows.experience = checkBlank(data[i][1])
					Risk_Adj.experience = checkBlank(data[i][2])
					csm.experience = checkBlank(data[i][3])

				if  "Contracts initially recognised in the period" in data[i][0] :
					EPV_Cashflows.contracts = checkBlank(data[i][1])
					Risk_Adj.contracts = checkBlank(data[i][2])
					csm.contracts = checkBlank(data[i][3])


				if  "Changes in estimates reflected in CSM" in data[i][0] :
					EPV_Cashflows.changes_in_estimates_reflected_in_csm = checkBlank(data[i][1])
					Risk_Adj.changes_in_estimates_reflected_in_csm = checkBlank(data[i][2])
					csm.changes_in_estimates_reflected_in_csm = checkBlank(data[i][3])

				if  "Changes in estimates resulting in onerous contract losses" in data[i][0] :
					EPV_Cashflows.changes_in_estimates_resulting_in_onerous_contract_losses = checkBlank(data[i][1])
					Risk_Adj.changes_in_estimates_resulting_in_onerous_contract_losses = checkBlank(data[i][2])
					csm.changes_in_estimates_resulting_in_onerous_contract_losses = checkBlank(data[i][3])

				if  "Adjustments to liabilities for incurred claims" in data[i][0] :
					EPV_Cashflows.adjustments = checkBlank(data[i][1])
					Risk_Adj.adjustments = checkBlank(data[i][2])
					csm.adjustments = checkBlank(data[i][3])

				if "Insurance finance expenses" in data[i][0]:
					EPV_Cashflows.insurance_finance_expenses = checkBlank(data[i][1])
					Risk_Adj.insurance_finance_expenses = checkBlank(data[i][2])
					csm.insurance_finance_expenses = checkBlank(data[i][3])

				if "Effect of movements in exchange rates" in data[i][0]:
					EPV_Cashflows.movements_in_exchange_rates = checkBlank(data[i][1])
					Risk_Adj.movements_in_exchange_rates = checkBlank(data[i][2])
					csm.movements_in_exchange_rates = checkBlank(data[i][3])

				if "Cashflows" in data[i][0]:
					EPV_Cashflows.cashflows = checkBlank(data[i][1])
					Risk_Adj.cashflows = checkBlank(data[i][2])
					csm.cashflows = checkBlank(data[i][3])

					rpt.epv_cashflows = EPV_Cashflows
					rpt.risk_adj = Risk_Adj
					rpt.csm = csm
					
					break	
		
		outputStream.write(rpt.toJSON().encode('utf-8'))


def checkBlank(input_str):
    try:
        input_str = float(input_str)
    except ValueError:
        input_str = 0
    return input_str


flowFile = session.get()
if (flowFile != None):
	flowFile = session.write(flowFile, ModJSON())
	flowFile = session.putAttribute(flowFile, "filename", flowFile.getAttribute('filename').split('.')[0]+'_translated.json')
session.transfer(flowFile, REL_SUCCESS)
session.commit()
