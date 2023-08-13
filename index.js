const fs = require("fs");
const https = require("https");
const perspective = require("@finos/perspective");
const XLSX = require("xlsx");
const URL = "https://community.tableau.com/servlet/JiveServlet/downloadBody/1236-102-2-15278/Sample%20-%20Superstore.xls";

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
            fs.writeFileSync("./superstore.feather", Buffer.from(arrow), "binary");
            console.log("Wrote " + arrow.byteLength + " bytes");
        });
    })
    .on("error", (error) => console.error(error));
} else {

    const arrow = fs.readFileSync("./superstore.arrow");
    perspective.table(SCHEMA).then(table => {
        table.update(arrow.buffer);
        return table;
    }).then(table => 
        table.view()
    ).then(view => 
        view.to_arrow()
    ).then(feather => 
        fs.writeFileSync("./superstore.feather", Buffer.from(feather), "binary")
    );

}
