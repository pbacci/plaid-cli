package main

import (
	"fmt"
	"os"
	"time"

	"github.com/brianloveswords/airtable"
	"github.com/plaid/plaid-go/plaid"
)

var _ = fmt.Println

type TransactionFields struct {
	PlaidID        string
	AccountID      string
	Amount         float64
	Name           string
	MerchantName   string
	Pending        bool
	DateTime       string
	Address        string
	City           string
	Country        string
	PostalCode     string
	PlaidCategory1 string
	PlaidCategory2 string
	PlaidCategory3 string
}

type TransactionRecord struct {
	airtable.Record
	Fields   TransactionFields
	Typecast bool
}

type AccountBalanceFields struct {
	AccountID           string
	CurrentPlaidBalance float64
	Date                string
}
type AccountBalanceRecord struct {
	airtable.Record
	Fields   AccountBalanceFields
	Typecast bool
}

func SyncTransactions(transactions []plaid.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	client := airtable.Client{
		APIKey: os.Getenv("AIRTABLE_API_KEY"),
		BaseID: os.Getenv("AIRTABLE_APP_ID"),
	}

	expenses := client.Table("Transactions")

	plaidTransactions := make([]TransactionRecord, len(transactions))
	for i, t := range transactions {
		s := func(tags []string, n int) string {
			if n >= len(tags) {
				return ""
			}
			return tags[n]
		}
		plaidTransactions[i] = TransactionRecord{Fields: TransactionFields{
			PlaidID:        t.ID,
			AccountID:      t.AccountID,
			Amount:         t.Amount,
			Name:           t.Name,
			MerchantName:   t.MerchantName,
			Pending:        t.Pending,
			DateTime:       t.Date,
			Address:        t.Location.Address,
			City:           t.Location.City,
			Country:        t.Location.Country,
			PostalCode:     t.Location.PostalCode,
			PlaidCategory1: s(t.Category, 0),
			PlaidCategory2: s(t.Category, 1),
			PlaidCategory3: s(t.Category, 2),
		}, Typecast: true}
	}
	plaidArranged := byAccountIDbyTransactionID(plaidTransactions)

	var airtableTransactions []TransactionRecord
	err := expenses.List(&airtableTransactions, &airtable.Options{})
	if err != nil {
		return err
	}

	airtableArranged := byAccountIDbyTransactionID(airtableTransactions)

	for accountID, transactions := range plaidArranged {
		updates := updateAccount(transactions, airtableArranged[accountID])

		for i, t := range updates.ToCreate {
			err := expenses.Create(&t)
			if err != nil {
				return err
			}
			fmt.Printf("Created %d/%d transactions\n", i+1, len(updates.ToCreate))
		}
	}

	return nil
}

func byAccountIDbyTransactionID(ts []TransactionRecord) map[string]map[string]TransactionRecord {
	ret := make(map[string]map[string]TransactionRecord)
	for _, t := range ts {
		byID, ok := ret[t.Fields.AccountID]
		if !ok {
			byID = make(map[string]TransactionRecord)
			ret[t.Fields.AccountID] = byID
		}
		byID[t.Fields.PlaidID] = t
	}
	return ret
}

type AccountUpdate struct {
	ToCreate []TransactionRecord
}

func updateAccount(plaidTs, airtableTs map[string]TransactionRecord) AccountUpdate {
	var u AccountUpdate
	ids := make(map[string]struct{})
	for id, t := range plaidTs {
		ids[id] = struct{}{}
		_, ok := airtableTs[id]
		if !ok && !t.Fields.Pending {
			u.ToCreate = append(u.ToCreate, t)
		}
	}

	return u
}

func SyncAccountBalances(accountBalances []plaid.Account) error {
	client := airtable.Client{
		APIKey: os.Getenv("AIRTABLE_API_KEY"),
		BaseID: os.Getenv("AIRTABLE_APP_ID"),
	}

	airtableBalancesTable := client.Table("Account Balances")
	currentDate := time.Now().Format("2006-01-02")

	for _, t := range accountBalances {
		var existingBalanceRecords []AccountBalanceRecord
		filterString := fmt.Sprintf("AND({AccountId} = \"%s\", DATETIME_FORMAT({Date}, \"YYYY-MM-DD\") = \"%s\")", t.AccountID, currentDate)
		err := airtableBalancesTable.List(&existingBalanceRecords, &airtable.Options{
			Filter: filterString,
		})
		if err != nil {
			return err
		}

		if len(existingBalanceRecords) == 0 {
			accountBalanceRecordToAdd := AccountBalanceRecord{Fields: AccountBalanceFields{
				AccountID:           t.AccountID,
				CurrentPlaidBalance: t.Balances.Current,
				Date:                currentDate,
			}, Typecast: true}

			err := airtableBalancesTable.Create(&accountBalanceRecordToAdd)
			if err != nil {
				return err
			}
			fmt.Printf("Created balance record for accountID %s\n", t.AccountID)
		} else {
			fmt.Printf("Found pre-existing balance record for accountID %s\n", t.AccountID)
		}
	}
	return nil
}
