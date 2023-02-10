# How to setup dbt cloud with Redshift

The original documentation can be found [here.](https://docs.getdbt.com/docs/get-started/connect-your-database#connecting-to-redshift-and-postgres)

With redshift it is not necessary to create a new AWS user, just to provide the credentials (username + password) to log in to the redshift cluster (see under week_1_basics_n_setup\1_terraform_aws\secrets.tf)
<td> <img src="imgs\Capture1.PNG" style="width: 800px;"/> </td>
<td> <img src="imgs\Capture3.PNG" style="width: 800px;"/> </td>
<td> <img src="imgs\Capture4.PNG" style="width: 400px;"/> </td>



**It is also necessary to modify the inbounds rules of the security group of the redshift cluster to allow access from the dbt cloud server. See it explained [here](https://docs.getdbt.com/docs/deploy/regions-ip-addresses) with the IP addresses shown [here](https://docs.getdbt.com/docs/deploy/regions-ip-addresses). >>> dbt Cloud will always connect to your warehouse from 52.45.144.63, 54.81.134.249, or 52.22.161.231. Make sure to allow inbound traffic from these IPs in your firewall, and include it in any database grants.** This has to be done even if the inbound rules allow all traffic.


The rest is the same as in week_4_analytics_engineering\dbt_cloud_setup.md



