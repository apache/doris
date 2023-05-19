with open('case.tmpl', 'r') as f:
    tmpl = f.read()
    for i in range(1,100):
       with open('query'+str(i)+'.sql', 'r') as fi:
        casei = tmpl.replace('{--}', str(i))
        casei = casei.replace('{query}', fi.read())

        with open('../shape/query'+str(i)+'.groovy', 'w') as out:
            out.write(casei)