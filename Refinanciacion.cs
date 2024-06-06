using BusComun;
using Comun;
using RSModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading;


namespace BusInchost
{
    public class Refinanciacion : cBase
    {
        RefinanciacionDalc dalc;
        cGlobales vGlobales;

        public Refinanciacion(cConexion pconn)
        {
            conn = pconn;
            dalc = new RefinanciacionDalc(conn);
            vGlobales = new cGlobales(conn);
        }

        public bool ProcesoRecalculoCuotasRefinanciacion()
        {
            try
            {
                DataSet ds = dalc.GetRefinanciacionDeudaVariada();

                foreach (DataRow dr in ds.Tables["REFINANCIACION"].Rows)
                {
                    // Si las obligaciones han regulairzado deuda omitimos el recálculo
                    // Adicionalmente, habría que comentar con COMAFI si hay que cancelar estas refinanciaciones
                    if (cFormat.StringToDouble(dr["SALDOTOTAL"].ToString()) == 0)
                        continue;

                    string idRefi = dr["RECOD"].ToString();
                    double saldoTotal = cFormat.StringToDouble(dr["SALDOTOTAL"].ToString());
                    double anticipos = cFormat.StringToDouble(dr["REPAGOINICIAL"].ToString());
                    double porQuita = cFormat.StringToDouble(dr["REPORQUITA"].ToString());
                    double porComision = cFormat.StringToDouble(dr["REPORCOM"].ToString());
                    string provinciaSellado = dr["REPROVSELLOS"].ToString();
                    string categoriaIVA = dr["REIVA"].ToString();
                    double porTasa = cFormat.StringToDouble(dr["REPORTASA"].ToString());
                    double porTasaPlantilla = cFormat.StringToDouble(dr["REPORTASAPLANTILLA"].ToString());
                    double totalRefinanciado = cFormat.StringToDouble(dr["RETOTAREFIN"].ToString());
                    int cantCuotas = Convert.ToInt32(dr["REPLAZO"].ToString());

                    if (!this.RecalculoCuotas(idRefi, saldoTotal, anticipos, porQuita, porComision, provinciaSellado, categoriaIVA, porTasa, porTasaPlantilla, cantCuotas, totalRefinanciado))
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                this.Errores.Agregar(999, "ModificarRefinanciacion", ex.Message, Const.SEVERIDAD_Alta);
                cIncidencia.Generar(this.Errores, "ModificarRefinanciacion", ex.Message);

                return false;
            }
            
        }

        public bool RecalculoCuotas(string idRefi, double saldoTotal, double anticipos, double porQuita, double porComision, string provinciaSellado, string categoriaIVA, double porTasa, double porTasaPlantilla, int cantCuotas, double totalRefinanciado)
        {
            try
            {    
                if (porQuita > 0)
                {
                    double montoPagare = saldoTotal - anticipos;
                    double quita = saldoTotal - totalRefinanciado;
                    porQuita = quita / saldoTotal * 100;

                    double montoRefiSinComision = saldoTotal - quita - anticipos;
                    double montoComision = porComision / 100 * montoRefiSinComision;
                    double montoRefinanciado = montoRefiSinComision + montoComision;

                    double montoSellados = this.CalcularSellados(saldoTotal, montoPagare, provinciaSellado);
                
                    if (!dalc.ModificarCuotaPorSellado(idRefi, montoSellados))
                    {
                        cIncidencia.Generar(dalc.Errores, "ModificarCuotaPorSellado", string.Format("Se ha producido un error al modificar la primera cuota de refinanciación {0} por variación de sellados - Id Refinanciación {0}", idRefi));
                        return false;
                    }

                    double[] flow = new double[cantCuotas + 1];
                    flow[0] = -montoRefinanciado;

                    DataSet dsCuotas = dalc.GetCuotas(idRefi);
                    int i = 0;
                    foreach (DataRow dr in dsCuotas.Tables["CUOTAS"].Rows)
                    {
                        i++;
                        double total = cFormat.StringToDouble(dr["RCTOTAL"].ToString());
                        flow[i] = total;
                    }

                    foreach (var d in flow)
                        cIncidencia.Aviso(d.ToString());

                    double tir = Excel.FinancialFunctions.Financial.Irr(flow);
                    double cft = (Math.Pow((1 + tir), (double)365 / 30) - 1) * 100;

                    if (!dalc.ModificarRefinanciacion(idRefi, saldoTotal, porQuita, quita, montoPagare, cft))
                    {
                        cIncidencia.Generar(dalc.Errores, "ModificarRefinanciacion", string.Format("Se ha producido un error al modificar los datos de refinanciación - Id Refinanciación {0}", idRefi));
                        return false;
                    }
                }
                else
                {
                    double montoPagare = saldoTotal - anticipos;
                    double quita = saldoTotal * porQuita / 100;
                    double montoRefiSinComision = saldoTotal - quita - anticipos;
                    double montoComision = porComision / 100 * montoRefiSinComision;
                    double montoRefinanciado = montoRefiSinComision + montoComision;

                    double montoSellados = this.CalcularSellados(saldoTotal, montoPagare, provinciaSellado);

                    DataSet dsIVA = dalc.TablaDeTablas(Catalogos.Condiciones_ante_el_IVA, Catalogos.Condiciones_ante_el_IVA.ToString(), ColumnsTABLFilter.TBCODE, categoriaIVA);
                    double porIVA = 0;
                    if (dsIVA.Tables[Catalogos.Condiciones_ante_el_IVA.ToString()].Rows.Count > 0)
                        porIVA = cFormat.StringToDouble(dsIVA.Tables[Catalogos.Condiciones_ante_el_IVA.ToString()].Rows[0]["TBAIS"].ToString());

                    double interesPeriodo = porTasa / 100 * 30 / 365;
                    double valorCuota = (Math.Pow(1 + interesPeriodo, cantCuotas) * interesPeriodo * montoRefinanciado) / (Math.Pow(1 + interesPeriodo, cantCuotas) - 1);

                    double porSeguro = cFormat.StringToDouble(vGlobales.GetValorRParam("PRSEGURO"));

                    double saldoPend = montoRefinanciado;
                    double totalSinIVA = 0;

                    double[] flow = new double[cantCuotas + 1];
                    flow[0] = -montoRefinanciado;

                    for (int i = 1; i <= cantCuotas; i++)
                    {
                        double interes = saldoPend * interesPeriodo;
                        double capital = valorCuota - interes;
                        double seguro = porSeguro / 100 * saldoPend / 1000;
                        double iva = (interes + seguro) * porIVA / 100;
                        double total = capital + interes + seguro + iva + (i == 1 ? montoSellados : 0);
                        totalSinIVA += interes + capital + seguro;
                        saldoPend = saldoPend - capital;

                        if (!dalc.ModificarCuotasRefinanciacion(idRefi, i, total, capital, interes, seguro, iva, i == 1 ? montoSellados : 0, saldoPend, capital))
                        {
                            cIncidencia.Generar(dalc.Errores, "ModificarCuotasRefinanciacion", string.Format("Se ha producido un error al actualizar la cuota de la refinanciación - Id Refinanciación {0} - Número Cuota {1}", idRefi, i));
                            return false;
                        }

                        flow[i] = total;
                    }

                    foreach (var d in flow)
                        cIncidencia.Aviso(d.ToString());

                    double tir = Excel.FinancialFunctions.Financial.Irr(flow);
                    double cft = (Math.Pow((1 + tir), (double)365 / 30) - 1) * 100;

                    double importeCuota = totalSinIVA / cantCuotas;

                    double tasa = 0;
                    double tasaPlantilla = 0;
                    this.CalcularTasas(porTasa, porTasaPlantilla, montoRefinanciado, cantCuotas, ref tasa, ref tasaPlantilla);

                    if (!dalc.ModificarRefinanciacion(idRefi, saldoTotal, quita, montoRefinanciado, importeCuota, montoComision, tasa, tasaPlantilla, montoSellados, montoPagare, cft))
                    {
                        cIncidencia.Generar(dalc.Errores, "ModificarRefinanciacion", string.Format("Se ha producido un error al modificar los datos de refinanciación - Id Refinanciación {0}", idRefi));
                        return false;
                    }
                }

                if (!dalc.ActualizarDeudaObligacionRefi(idRefi))
                {
                    cIncidencia.Generar(dalc.Errores, "ModificarRefinanciacion", string.Format("Se ha producido un error al modificar los datos de refinanciación - Id Refinanciación {0}", idRefi));
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                this.Errores.Agregar(999, "ModificarRefinanciacion", ex.Message, Const.SEVERIDAD_Alta);
                cIncidencia.Generar(this.Errores, "ModificarRefinanciacion", ex.Message);

                cIncidencia.Generar(this.Errores, "ModificarRefinanciacion", string.Format("Se ha producido un error al modificar los datos de refinanciación - Id Refinanciación {0}", idRefi));

                return false;
                
            }

        }

        private void CalcularTasas(double porTasa, double porTasaPlantilla, double montoRefinanciado, int cantCuotas, ref double tasa, ref double tasaPlantilla)
        {
            double interesPeriodo = porTasa / 100 * 30 / 365;
            double interesPeriodoPlantilla = porTasaPlantilla / 100 * 30 / 365;

            double valorCuota = (Math.Pow(1 + interesPeriodo, cantCuotas) * interesPeriodo * montoRefinanciado) / (Math.Pow(1 + interesPeriodo, cantCuotas) - 1);
            double valorCuotaPlantilla = (Math.Pow(1 + interesPeriodoPlantilla, cantCuotas) * interesPeriodoPlantilla * montoRefinanciado) / (Math.Pow(1 + interesPeriodoPlantilla, cantCuotas) - 1);

            double saldoPend = montoRefinanciado;
            double saldoPendPlantilla = montoRefinanciado;

            for (int i = 1; i <= cantCuotas; i++)
            {
                double interes = saldoPend * interesPeriodo;
                double interesPlantilla = saldoPendPlantilla * interesPeriodoPlantilla;

                double capital = valorCuota - interes;
                double capitalPlantilla = valorCuota - interesPlantilla;

                tasa += interes;
                tasaPlantilla += interesPlantilla;

                saldoPend = saldoPend - capital;
                saldoPendPlantilla = saldoPendPlantilla - capitalPlantilla;
            }
        }

        private double CalcularSellados(double saldoTotal, double montoPagare, string provinciaSellado)
        {
            DataSet dsConfigSellados = dalc.TablaDeTablas(Catalogos.Configuracion_Sellados, Catalogos.Configuracion_Sellados.ToString(), ColumnsTABLFilter.TBCODE, provinciaSellado);

            double montoSellados = 0;
            if (dsConfigSellados.Tables[Catalogos.Configuracion_Sellados.ToString()].Rows.Count > 0)
            {
                CultureInfo culture = new CultureInfo("ar");
                culture.NumberFormat.NumberDecimalSeparator = ",";

                DataRow drConfigSellados = dsConfigSellados.Tables[Catalogos.Configuracion_Sellados.ToString()].Rows[0];
                double alicuotaRec = cFormat.StringToDouble(drConfigSellados["TBAIS2"].ToString(), culture);
                double impuestoMinRec = cFormat.StringToDouble(drConfigSellados["TBLINK"].ToString(), culture);
                double alicuotaPag = cFormat.StringToDouble(drConfigSellados["TBABREV"].ToString(), culture);
                double impuestoMinPag = cFormat.StringToDouble(drConfigSellados["TBAIS"].ToString(), culture);

                double valorAlicuotaRec = saldoTotal / 1000 * alicuotaRec;
                double valorAlicuotaPag = montoPagare / 1000 * alicuotaPag;

                valorAlicuotaRec = valorAlicuotaRec < impuestoMinRec ? impuestoMinRec : valorAlicuotaRec;
                valorAlicuotaPag = valorAlicuotaPag < impuestoMinPag ? impuestoMinPag : valorAlicuotaPag;

                montoSellados = valorAlicuotaRec + valorAlicuotaPag;
            }

            return montoSellados;
        }

        private string SumarMes(string fecha, int offset)
        {
            DateTime date = Convert.ToDateTime(cFormat.FechaBDToPc(fecha));
            DateTime dateAux = Convert.ToDateTime(cFormat.FechaBDToPc(fecha)).AddMonths(offset);

            if (offset != 0)
            {
                int year = dateAux.Year;
                int month = dateAux.Month;

                if ((dateAux.Year * 12 + dateAux.Month) - (date.Year * 12 + date.Month) > offset)
                    dateAux = new DateTime(dateAux.Year, dateAux.Month, 1).AddDays(-1);
            }

            return dateAux.ToString("yyyyMMdd");
        }
    }
}
