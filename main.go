package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func main() {
	const addr = "127.0.0.1:9090"

	payloads := []string{
		`x:CREATE OR REPLACE SECRET (
  TYPE gcs,
  KEY_ID '` + os.Getenv("LUNA_GCS_HMAC_KEY") + `',
  SECRET '` + os.Getenv("LUNA_GCS_HMAC_SECRET") + `'
);`,
		`x:CREATE TABLE tmpcur AS FROM
read_csv('gs://awscur/992382443124_2025-08*.csv',
header = true,
union_by_name = true,
files_to_sniff = -1,
types = {
  'uuid':'VARCHAR',
  'date':'DATE',
  'payer':'VARCHAR',
  'pricing/LeaseContractLength':'VARCHAR',
  'pricing/OfferingClass':'VARCHAR',
  'pricing/PurchaseOption':'VARCHAR',
  'reservation/AvailabilityZone':'VARCHAR',
  'reservation/ReservationARN':'VARCHAR',
  'savingsPlan/Region':'VARCHAR',
  'savingsPlan/PaymentOption':'VARCHAR',
  'savingsPlan/EndTime':'VARCHAR',
  'savingsPlan/InstanceTypeFamily':'VARCHAR',
  'savingsPlan/PurchaseTerm':'VARCHAR',
  'savingsPlan/OfferingType':'VARCHAR',
  'savingsPlan/StartTime':'VARCHAR',
  'identity/LineItemId':'VARCHAR',
  'identity/TimeInterval':'VARCHAR',
  'bill/InvoiceId':'VARCHAR',
  'bill/InvoicingEntity':'VARCHAR',
  'bill/BillingEntity':'VARCHAR',
  'bill/BillType':'VARCHAR',
  'bill/PayerAccountId':'VARCHAR',
  'bill/BillingPeriodStartDate':'TIMESTAMP',
  'bill/BillingPeriodEndDate':'TIMESTAMP',
  'lineItem/UsageAccountId':'VARCHAR',
  'lineItem/LineItemType':'VARCHAR',
  'lineItem/UsageStartDate':'TIMESTAMP',
  'lineItem/UsageEndDate':'TIMESTAMP',
  'lineItem/ProductCode':'VARCHAR',
  'lineItem/UsageType':'VARCHAR',
  'lineItem/Operation':'VARCHAR',
  'lineItem/AvailabilityZone':'VARCHAR',
  'lineItem/ResourceId':'VARCHAR',
  'lineItem/UsageAmount':'DOUBLE',
  'lineItem/NormalizationFactor':'DOUBLE',
  'lineItem/NormalizedUsageAmount':'DOUBLE',
  'lineItem/CurrencyCode':'VARCHAR',
  'lineItem/UnblendedRate':'VARCHAR',
  'lineItem/UnblendedCost':'DOUBLE',
  'lineItem/BlendedRate':'VARCHAR',
  'lineItem/BlendedCost':'DOUBLE',
  'lineItem/LineItemDescription':'VARCHAR',
  'lineItem/TaxType':'VARCHAR',
  'lineItem/LegalEntity':'VARCHAR',
  'product/ProductName':'VARCHAR',
  'product/alarmType':'VARCHAR',
  'product/availability':'VARCHAR',
  'product/availabilityZone':'VARCHAR',
  'product/capacitystatus':'VARCHAR',
  'product/classicnetworkingsupport':'VARCHAR',
  'product/clockSpeed':'VARCHAR',
  'product/currentGeneration':'VARCHAR',
  'product/databaseEngine':'VARCHAR',
  'product/dedicatedEbsThroughput':'VARCHAR',
  'product/deploymentOption':'VARCHAR',
  'product/description':'VARCHAR',
  'product/durability':'VARCHAR',
  'product/ecu':'VARCHAR',
  'product/engineCode':'VARCHAR',
  'product/enhancedNetworkingSupported':'VARCHAR',
  'product/eventType':'VARCHAR',
  'product/feeCode':'VARCHAR',
  'product/feeDescription':'VARCHAR',
  'product/fromLocation':'VARCHAR',
  'product/fromLocationType':'VARCHAR',
  'product/fromRegionCode':'VARCHAR',
  'product/gpuMemory':'VARCHAR',
  'product/group':'VARCHAR',
  'product/groupDescription':'VARCHAR',
  'product/instanceFamily':'VARCHAR',
  'product/instanceType':'VARCHAR',
  'product/instanceTypeFamily':'VARCHAR',
  'product/intelAvx2Available':'VARCHAR',
  'product/intelAvxAvailable':'VARCHAR',
  'product/intelTurboAvailable':'VARCHAR',
  'product/licenseModel':'VARCHAR',
  'product/location':'VARCHAR',
  'product/locationType':'VARCHAR',
  'product/logsDestination':'VARCHAR',
  'product/marketoption':'VARCHAR',
  'product/maxIopsvolume':'VARCHAR',
  'product/maxThroughputvolume':'VARCHAR',
  'product/maxVolumeSize':'VARCHAR',
  'product/memory':'VARCHAR',
  'product/messageDeliveryFrequency':'VARCHAR',
  'product/messageDeliveryOrder':'VARCHAR',
  'product/networkPerformance':'VARCHAR',
  'product/normalizationSizeFactor':'VARCHAR',
  'product/operatingSystem':'VARCHAR',
  'product/operation':'VARCHAR',
  'product/physicalProcessor':'VARCHAR',
  'product/preInstalledSw':'VARCHAR',
  'product/processorArchitecture':'VARCHAR',
  'product/processorFeatures':'VARCHAR',
  'product/productFamily':'VARCHAR',
  'product/queueType':'VARCHAR',
  'product/region':'VARCHAR',
  'product/regionCode':'VARCHAR',
  'product/requestType':'VARCHAR',
  'product/servicecode':'VARCHAR',
  'product/servicename':'VARCHAR',
  'product/sku':'VARCHAR',
  'product/storage':'VARCHAR',
  'product/storageClass':'VARCHAR',
  'product/storageMedia':'VARCHAR',
  'product/tenancy':'VARCHAR',
  'product/toLocation':'VARCHAR',
  'product/toLocationType':'VARCHAR',
  'product/toRegionCode':'VARCHAR',
  'product/transferType':'VARCHAR',
  'product/type':'VARCHAR',
  'product/usagetype':'VARCHAR',
  'product/vcpu':'VARCHAR',
  'product/version':'VARCHAR',
  'product/volumeApiName':'VARCHAR',
  'product/volumeType':'VARCHAR',
  'product/vpcnetworkingsupport':'VARCHAR',
  'pricing/RateCode':'VARCHAR',
  'pricing/RateId':'VARCHAR',
  'pricing/currency':'VARCHAR',
  'pricing/publicOnDemandCost':'DOUBLE',
  'pricing/publicOnDemandRate':'VARCHAR',
  'pricing/term':'VARCHAR',
  'pricing/unit':'VARCHAR',
  'reservation/AmortizedUpfrontCostForUsage':'DOUBLE',
  'reservation/AmortizedUpfrontFeeForBillingPeriod':'DOUBLE',
  'reservation/EffectiveCost':'DOUBLE',
  'reservation/EndTime':'VARCHAR',
  'reservation/ModificationStatus':'VARCHAR',
  'reservation/RecurringFeeForUsage':'DOUBLE',
  'reservation/StartTime':'VARCHAR',
  'reservation/SubscriptionId':'VARCHAR',
  'reservation/TotalReservedNormalizedUnits':'VARCHAR',
  'reservation/TotalReservedUnits':'VARCHAR',
  'reservation/UnitsPerReservation':'VARCHAR',
  'reservation/UnusedAmortizedUpfrontFeeForBillingPeriod':'DOUBLE',
  'reservation/UnusedNormalizedUnitQuantity':'DOUBLE',
  'reservation/UnusedQuantity':'DOUBLE',
  'reservation/UnusedRecurringFee':'DOUBLE',
  'reservation/UpfrontValue':'DOUBLE',
  'savingsPlan/TotalCommitmentToDate':'DOUBLE',
  'savingsPlan/SavingsPlanARN':'VARCHAR',
  'savingsPlan/SavingsPlanRate':'DOUBLE',
  'savingsPlan/UsedCommitment':'DOUBLE',
  'savingsPlan/SavingsPlanEffectiveCost':'DOUBLE',
  'savingsPlan/AmortizedUpfrontCommitmentForBillingPeriod':'DOUBLE',
  'savingsPlan/RecurringCommitmentForBillingPeriod':'DOUBLE',
  'tags':'VARCHAR',
  'costcategories':'VARCHAR'
});`,
		`q:DESCRIBE tmpcur;`,
		`q:SELECT uuid, date, payer FROM tmpcur;`,
	}

	for _, payload := range payloads {
		func() {
			log.Printf("Go Arrow client connecting to %s", addr)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatalf("Failed to connect to server: %v", err)
			}

			defer conn.Close()
			log.Println("Successfully connected.")

			fpayload := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
			_, err = conn.Write([]byte(fpayload))
			if err != nil {
				log.Fatalf("Failed to send greeting: %v", err)
			}

			r, err := ipc.NewReader(conn)
			if err != nil {
				log.Printf("Failed to create Arrow reader: %v", err)
				return
			}

			defer r.Release()
			log.Println("Received schema:", r.Schema())

			var recordCount int
			for r.Next() {
				func() {
					rec := r.RecordBatch()
					defer rec.Release()

					recordCount++
					log.Printf("--- Reading Record Batch #%d ---", recordCount)
					log.Printf("Rows: %d, Columns: %d", rec.NumRows(), rec.NumCols())
					fmt.Println(rec)
				}()
			}

			if err := r.Err(); err != nil && err != io.EOF {
				log.Fatalf("Error reading records: %v", err)
			}

			log.Printf("Finished reading %d record batches from stream.", recordCount)
		}()
	}
}
