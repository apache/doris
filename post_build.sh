
cp -r fe/fe-core/target/generated-sources/annotations/* fe/fe-core/target/generated-sources
cp -r fe/fe-core/target/generated-sources/cup/* fe/fe-core/target/generated-sources
rm -rf fe/fe-core/target/generated-sources/cup/*
rm -rf fe/fe-core/target/generated-sources/annotations/* 
cp output/fe/lib/help-resource.zip fe/target/classes/
cp output/fe/lib/help-resource.zip fe/fe-core/target/classes/
