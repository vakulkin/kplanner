function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Keywords Filter')
      .addItem('Filter Keywords', 'myFunction')
      .addToUi();
}

function onChange(e) {
  myFunction();
}

function myFunction() {
  
}
