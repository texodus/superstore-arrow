import * as fs from "fs";
import * as https from "https";
import * as perspective from "@finos/perspective";
import * as XLSX from "xlsx";
import duckdb from "duckdb";
const URL =
    "https://community.tableau.com/servlet/JiveServlet/downloadBody/1236-102-2-15278/Sample%20-%20Superstore.xls";

const SCHEMA = {
    "Row ID": "integer",
    "Order ID": "string",
    "Order Date": "date",
    "Ship Date": "date",
    "Ship Mode": "string",
    "Customer ID": "string",
    "Customer Name": "string",
    Segment: "string",
    Country: "string",
    City: "string",
    State: "string",
    "Postal Code": "float",
    Region: "string",
    "Product ID": "string",
    Category: "string",
    "Sub-Category": "string",
    "Product Name": "string",
    Sales: "float",
    Quantity: "integer",
    Discount: "float",
    Profit: "float",
};

// Bootstrap
if (false) {
    https
        .get(URL, (res) => {
            console.log(`Status ${res.statusCode}`);
            let body = [];
            res.on("data", (data) => {
                body.push(data);
            });
            res.on("end", async () => {
                console.log(`Downloaded`);
                fs.writeFileSync("./superstore.xls", Buffer.concat(body));
                const xls = XLSX.readFile("./superstore.xls");
                console.log("Read XLS", xls.SheetNames);

                const csv = XLSX.utils.sheet_to_csv(xls.Sheets["Orders"]);
                console.log("Encode to csv");

                const table = perspective.table(SCHEMA);
                await table.update(csv);
                const arrow = await table.view().to_arrow();
                fs.writeFileSync(
                    "./superstore.feather",
                    Buffer.from(arrow),
                    "binary",
                );
                console.log("Wrote " + arrow.byteLength + " bytes");
            });
        })
        .on("error", (error) => console.error(error));
} else {
    // Build on previous version
    const arrow = fs.readFileSync("./superstore.arrow");
    perspective
        .table(SCHEMA)
        .then((table) => {
            table.update(arrow.buffer);
            return table;
        })
        .then((table) => table.view())
        .then((view) =>
            Promise.all([
                view
                    .to_arrow()
                    .then((feather) =>
                        fs.writeFileSync(
                            "./superstore.lz4.arrow",
                            Buffer.from(feather),
                            "binary",
                        ),
                    ),
                view.to_csv().then((csv) => {
                    fs.writeFileSync("./superstore.csv", csv);
                    const db = new duckdb.Database(":memory:");
                    db.all(
                        `COPY (
                            SELECT * from read_csv(
                                "superstore.csv",
                                types = {
                                    'Row ID': 'INTEGER',
                                    'Quantity': 'INTEGER',
                                    'Postal Code': 'INTEGER'
                                }
                            )
                        ) TO "superstore.parquet" (
                            FORMAT "parquet", 
                            COMPRESSION "ZSTD"
                        )`,
                        function (err, res) {
                            if (err) {
                                console.warn(err);
                            }
                            console.log(res);
                        },
                    );
                }),
            ]),
        );
}
